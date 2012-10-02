#simple python script to fix early entries in githubarchive that 
#did not use newline as delimiter between json entries

import glob, sys
inFile,outFile = sys.argv[1:]

inFileHandle = open(inFile,'r')
rawText = inFileHandle.read()
inFileHandle.close()

#only fix if newlines missing
if rawText.count('\n') == 0:
    print 'fixing '+  inFile
    outFileHandle = open(outFile,'w')
    
    lastChar = ''
    chunk = ''
    
    for currentChar in rawText:
        if lastChar + currentChar == '}{':
            outFileHandle.write(chunk+'\n')
            chunk = ''
        chunk += currentChar
        lastChar = currentChar

    outFileHandle.close()

else: 
    print inFile + ' already uses newline delimiters'
