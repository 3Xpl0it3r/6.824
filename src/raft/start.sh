Task=$1
Workers=$2
Itera=$3

Test2A="TestInitialElection2A TestReElection2A TestManyElections2A "
Test2B="TestBasicAgree2B TestRPCBytes2B TestFor2023TestFollowerFailure2B TestFor2023TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B  "
Test2C="TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C TestReliableChurn2C TestUnreliableChurn2C"
Test2D=" TestSnapshotBasic2D TestSnapshotInstall2D TestSnapshotInstallUnreliable2D TestSnapshotInstallCrash2D TestSnapshotInstallUnCrash2D TestSnapshotAllCrash2D "

if [ "${Task}" = "all" ]
then
    Task="${Tesk2A}  ${Test2B} ${Test2C} ${Test2D}"
fi


VERBOSE=1 python3 dstest.py ${Task} -p ${Workers} -n ${Itera}
