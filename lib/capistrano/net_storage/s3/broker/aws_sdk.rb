require 'aws-sdk-s3'
require 'capistrano/net_storage/s3/base'
require 'capistrano/net_storage/s3/broker/base'

class Capistrano::NetStorage::S3::Broker::AwsSdk < Capistrano::NetStorage::S3::Broker::Base
  def check
  end

  def find_uploaded
  end

  def upload
  end

  def download
  end

  private

end
