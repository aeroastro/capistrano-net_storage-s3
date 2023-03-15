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
        Retriable.retriable on: [Aws::S3::Errors::ServiceError], tries: 3, base_interval: 5 do
          @s3.get_object({ bucket: c.net_storage_s3_bucket, key: c.archive_url }, target: ns.archive_path)
        end
      end
    end
  end

  def cleanup
    c = config
    s3_client = Aws::S3::Client.new(
      access_key_id: c.aws_access_key_id,
      secret_access_key: c.aws_secret_access_key,
      session_token: c.aws_session_token,
      region: c.aws_region
    )
    resp = s3_client.list_objects(bucket: c.bucket, prefix: c.archives_directory)
    objects = resp.contents.sort_by { |obj| obj.last_modified }
    c.s3_keep_releases.times { objects.pop }
    objects.each do |obj|
      s3_client.delete_object(bucket: c.bucket, key: obj.key)
    end
  end
end
