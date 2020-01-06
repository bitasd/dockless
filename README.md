# dockless






check out the command line's help with `python processlime.py` -- which is going to call a `python processlime.py --help`

for making it into a foor loop, do something like this: 


say for 421_440 441_460 which I had here a 5 row sample ...


```
for tgp in '421_440' '441_460' 
do
	python processlime.py --tgp=$tgp --work_dir='/home/arminakvn/Google Drive/processlime/' clean
done


for tgp in '421_440' '441_460' 
do
	python processlime.py --tgp=$tgp --work_dir='/home/arminakvn/Google Drive/processlime/' match
done


for tgp in '421_440' '441_460' 
do
	python processlime.py --tgp=$tgp --work_dir='/home/arminakvn/Google Drive/processlime/' nmatch
done


for tgp in '421_440' '441_460' 
do
	python processlime.py --tgp=$tgp --work_dir='/home/arminakvn/Google Drive/processlime/' zerod
done
```


```


time python3 processlime.py --tgp=461_480 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ clean


time python3 processlime.py --tgp=461_480 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ match

time python3 processlime.py --tgp=461_480 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ nmatch

time python3 processlime.py --tgp=461_480 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ zerod




time python3 processlime.py --tgp=481_500 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ clean


time python3 processlime.py --tgp=481_500 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ match

time python3 processlime.py --tgp=481_500 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ nmatch

time python3 processlime.py --tgp=481_500 --work_dir=/mnt/c/Users/bitas/folders/MAPC/codes/processlime/ zerod



```
