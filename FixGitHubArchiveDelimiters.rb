require 'yajl'
require 'zlib'

gzInputFile = open(ARGV[0])
jsonInputFile = Zlib::GzipReader.new(gzInputFile).read
outputFile = File.open(ARGV[1], 'w')

Yajl::Parser.parse(jsonInputFile) do |entry|
    outputFile << Yajl::Encoder.encode(entry)
    outputFile << "\n"
end
