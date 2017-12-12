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

SGE_ROOT="./"
export SGE_ROOT

chmod -x test_script.sh
ls -l test_script.sh
result=$(../fsl_sub -v ./test_script.sh 2>&1)
status=$?
echo "Test 1"
if [ $status -ne $NO_EXECUTABLE ]; then
    echo "Failed - status is $status, should be $NO_EXCUTABLE"
    echo "$result"
else
    if [ "$result" != "The command you have requested cannot be found or is not executable" ]; then
        echo "Failed ($result)"
    else
        echo "Passed"    
    fi
fi
echo "Test 2"
chmod +x test_script.sh
result=$(../fsl_sub test_script.sh 2>&1)
status=$?
if [ $status -ne $NO_EXECUTABLE ]; then
    echo "Failed"
else
    if [ "$result" != "The command you have requested cannot be found or is not executable" ]; then
        echo "Failed ($result)"
    else
        echo "Passed"
    fi
fi
echo "Test 3"
chmod -x test_script.sh
result=$(../fsl_sub -t test_parallel 2>&1)
status=$?
if [ $status -ne $NO_EXECUTABLE ]; then
    echo "Failed (status was $status, should be $NO_EXECUTABLE"
else
    if [ "$result" != "The command ./test_script.sh in the task file test_parallel, line 1 cannot be found or is not executable" ]; then
        echo "Failed ($result)"
    else
        echo "Passed"
    fi
fi

echo "Test 4"
chmod +x test_script.sh
result=$(../fsl_sub -t test_parallel_fail_1 2>&1)
status=$?
if [ $status -ne $NO_EXECUTABLE ]; then
    echo "Failed"
else
    if [ "$result" != "The command test_script.sh in the task file test_parallel_fail_1, line 2 cannot be found or is not executable" ]; then
        echo "Failed ($result)"
    else
        echo "Passed"
    fi
fi

echo "Test 5"
chmod +x test_script.sh
result=$(../fsl_sub -t test_parallel_fail_2 2>&1)
status=$?
if [ $status -ne $NO_EXECUTABLE ]; then
    echo "Failed"
else
    if [ "$result" != "The command test_script.sh in the task file test_parallel_fail_2, line 2 cannot be found or is not executable" ]; then
        echo "Failed ($result)"
    else
        echo "Passed"
    fi
fi

