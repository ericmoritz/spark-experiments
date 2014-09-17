import sys
import urllib
fh = urllib.urlopen(sys.argv[2])
count = int(sys.argv[1])
i = 0
for line in fh:
    print line,
    i+=1
    if i > count:
        break

