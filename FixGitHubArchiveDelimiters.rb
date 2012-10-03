# gem install yajl-ruby

require 'zlib'
require 'yajl'

gz = open(ARGV[0])
js = Zlib::GzipReader.new(gz).read
outFile = File.open(ARGV[1], 'w')

Yajl::Parser.parse(js) do |event|
    outFile << event
    outFile << "\n"
end
