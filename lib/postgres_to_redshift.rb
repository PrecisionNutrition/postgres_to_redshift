require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'tempfile'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri
  end

  attr_reader :source_connection, :target_connection, :s3

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.update_tables
    update_tables = PostgresToRedshift.new

    update_tables.tables.each do |table|
      target_connection.exec("CREATE TABLE IF NOT EXISTS public.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      update_tables.copy_table(table)

      update_tables.import_table(table)
    end

    post_update
  end

  def self.post_update
    puts "Running post update sql at ./post.sql"
    post_sql = File.read("./post.sql")

    puts target_connection.exec(post_sql)
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'").map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      next if (table.name =~ /^pg_/ || table.name == 'events' || table.name == 'versions')
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end

  def copy_table(table)
    tmpfile = Tempfile.new("psql2rs")     
	File.chmod(0666, tmpfile) 
    chunk = 1
    bucket.objects.with_prefix("export/#{table.target_table_name}.psv.gz").delete_all    	
	
    begin
      puts "Downloading #{table}"      
	  
	  copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO '#{tmpfile.path}' WITH DELIMITER '|'"
	  
	  source_connection.exec copy_command
	  
	  `/bin/gzip #{tmpfile.path} && mv #{tmpfile.path}.gz #{tmpfile.path}`
	  
	  File.chmod(0666, tmpfile) 
      
      upload_table(table, tmpfile, chunk)	       
    ensure            
      tmpfile.unlink
    end
  end

  def upload_table(table, file, chunk)		
    puts "Uploading #{table.target_table_name}.#{chunk}"	
    bucket.objects["export/#{table.target_table_name}.psv.gz.#{chunk}"].write(file: file, acl: :authenticated_read)
  end

  def import_table(table)
    puts "Importing #{table.target_table_name}"
    target_connection.exec("DROP TABLE IF EXISTS public.#{table.target_table_name}_updating")

    target_connection.exec("BEGIN;")

    target_connection.exec("ALTER TABLE public.#{target_connection.quote_ident(table.target_table_name)} RENAME TO #{table.target_table_name}_updating")

    target_connection.exec("CREATE TABLE public.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

    target_connection.exec("COPY public.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

    target_connection.exec("COMMIT;")
  end
end
