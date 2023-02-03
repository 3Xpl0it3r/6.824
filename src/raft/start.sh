Task=$1
Workers=$2
Itera=$3

Test2A="TestInitialElection2A TestReElection2A TestManyElections2A "
Test2B="TestBasicAgree2B TestRPCBytes2B TestFor2023TestFollowerFailure2B TestFor2023TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B  "


if [ "${Task}" = "all" ]
then
    Task="${Tesk2A}  ${Test2B}"
fi


VERBOSE=1 python3 dstest.py ${Task} -p ${Workers} -n ${Itera}
