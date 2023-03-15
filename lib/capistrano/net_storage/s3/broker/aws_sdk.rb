require 'aws-sdk-s3'
require 'capistrano/net_storage/s3/base'
require 'capistrano/net_storage/s3/broker/base'

class Capistrano::NetStorage::S3::Broker::AwsSdk < Capistrano::NetStorage::S3::Broker::Base
  def initialize
    c = config
    @s3 = Aws::S3::Client.new(
      access_key_id: c.net_storage_s3_aws_access_key_id,
      secret_access_key: c.net_storage_s3_aws_secret_access_key,
      region: c.net_storage_s3_aws_region
    )
  end

  def check
    @s3.head_bucket(bucket: config.net_storage_s3_bucket)
  rescue Aws::S3::Errors::NotFound
    raise "Bucket not found: #{config.net_storage_s3_bucket}"
  end

  def find_uploaded
    begin
      @s3.head_object(bucket: config.net_storage_s3_bucket, key: config.archive_url)
      set :net_storage_uploaded_archive, true
    rescue Aws::S3::Errors::NotFound
      set :net_storage_uploaded_archive, false
    end
  end

  def upload
    c = config
    ns = net_storage
    @s3.put_object(bucket: c.net_storage_s3_bucket, key: c.archive_url, body: File.open(ns.local_archive_path))
  end

  def download
    c = config
    ns = net_storage
    on ns.servers, in: :groups, limit: ns.max_parallels do
      within releases_path do
        @s3.get_object({ bucket: c.net_storage_s3_bucket, key: c.archive_url }, target: ns.archive_path)
      end
    end
  end
end
