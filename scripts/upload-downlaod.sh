#!/bin/bash

python scripts/clear_all.py rsync.test.1 root
rm -rf root1 db1 root2 db2
python example.py upload_new file1.txt
cp -r root root1 && cp -r db db1
python example.py upload_again ./root/file1.txt
cp -r root root2 && cp -r db db2
rm -rf db root && cp -r db1 db && cp -r root1 root
python example.py download_new_version ./root/file1.txt
diff root/file1.txt root2/file1.txt
