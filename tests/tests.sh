#!/bin/bash
PATH="$(pwd)/bin:$PATH"
export PATH
echo "Starting tests"

# Exit states
INVALID_SYNTAX="64"
NO_CUDA="65"
NO_CUDA_TYPE="66"
MISS_CONFIGURED="67"
ARG_MISSING_VALUE="68"
MISSING_FILE="69"
NO_CUDA_VERSION="70"
NO_EXECUTABLE="71"
BAD_QUEUE="72"

chmod -x test_script.sh
result=$(bash -x ../fsl_sub ./test_script.sh)
status=$?
if [ $status -eq $NO_EXECUTABLE ]; then
    echo "Test 1 failed - status is $status, should be 126"
    echo $result
else
    if [ "$result" != "The command you have requested cannot be found or is not executable" ]; then
        echo "Test 1 failed"
    fi
fi

chmod +x test_script.sh
result=$(../fsl_sub test_script.sh 2>&1)
status=$?
if [ $status -eq $NO_EXECUTABLE ]; then
    echo "Test 2 failed"
else
    if [ "$result" != "The command you have requested cannot be found or is not executable" ]; then
        echo "Test 2 failed"
    fi
fi

chmod -x test_script.sh
result=$(../fsl_sub -t test_parallel 2>&1)
status=$?
if [ $status -eq $NO_EXECUTABLE ]; then
    echo "Test 3 failed"
else
    if [ "$result" != "The command test_script.sj in the task file test_parallel, line 1 cannot be found or is not executable" ]; then
        echo "Test 3 failed"
    fi
fi

chmod +x test_script.sh
result=$(../fsl_sub -t test_parallel_fail_1 2>&1)
status=$?
if [ $status -eq $NO_EXECUTABLE ]; then
    echo "Test 4 failed"
else
    if [ "$result" != "The command test_script.sj in the task file test_parallel, line 2 cannot be found or is not executable" ]; then
        echo "Test 4 failed"
    fi
fi

chmod +x test_script.sh
result=$(../fsl_sub -t test_parallel_fail_2 2>&1)
status=$?
if [ $status -eq $NO_EXECUTABLE ]; then
    echo "Test 5 failed"
else
    if [ "$result" != "The command test_script.sj in the task file test_parallel, line 2 cannot be found or is not executable" ]; then
        echo "Test 5 failed"
    fi
fi