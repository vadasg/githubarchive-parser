# gem install yajl-ruby

require 'zlib'
require 'yajl'

gz = open(ARGV[0])
js = Zlib::GzipReader.new(gz).read

Yajl::Parser.parse(js) do |event|
  print event
  print "\n"
  end
