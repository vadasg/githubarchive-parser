#!/bin/sh
mkdir githubarchive
cd githubarchive
for i in {3..10}; do wget -q http://data.githubarchive.org/2012-`printf "%02d" $i`-{01..31}-{0..23}.json.gz & done
cd ..

sudo apt-get -y install maven2 ruby ruby-dev rubygems1.8 git-core
sudo gem install yajl-ruby
git clone https://github.com/thinkaurelius/titan.git
git clone https://github.com/vadasg/githubarchive-parser.git

echo 'to build titan: modify pom.xml then do'
echo 'mvn clean install -Dmaven.test.skip=true'
