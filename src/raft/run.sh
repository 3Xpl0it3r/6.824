if [[ $2 == 0 ]]
then
VERBOSE=1 go test -run $1  
else
VERBOSE=1 go test -run $1   | python3 dslogs.py -c 3
fi

