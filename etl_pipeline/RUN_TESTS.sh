#!/bin/bash
# Script to run all ETL pipeline tests

echo "======================================"
echo "ETL Pipeline Test Suite"
echo "======================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Activate virtual environment if it exists
if [ -f "/home/logi/txtai/venv/bin/activate" ]; then
    source /home/logi/txtai/venv/bin/activate
    echo "✓ Virtual environment activated"
else
    echo "⚠ Warning: Virtual environment not found, using system python3"
fi

PYTHON_CMD="python3"
if [ -f "/home/logi/txtai/venv/bin/python3" ]; then
    PYTHON_CMD="/home/logi/txtai/venv/bin/python3"
fi

echo "Python: $PYTHON_CMD"
echo ""

# Change to ETL pipeline directory
cd "$(dirname "$0")"

# Run module tests
echo "Running Module Tests..."
echo "--------------------------------------"
$PYTHON_CMD tests/test_modules.py
MODULE_EXIT=$?

echo ""
echo ""

# Run integration tests
echo "Running Integration Tests..."
echo "--------------------------------------"
$PYTHON_CMD tests/test_integration.py
INTEGRATION_EXIT=$?

echo ""
echo ""

# Summary
echo "======================================"
echo "Test Summary"
echo "======================================"

if [ $MODULE_EXIT -eq 0 ]; then
    echo -e "${GREEN}✓ Module Tests: PASSED${NC}"
else
    echo -e "${RED}✗ Module Tests: FAILED${NC}"
fi

if [ $INTEGRATION_EXIT -eq 0 ]; then
    echo -e "${GREEN}✓ Integration Tests: PASSED${NC}"
else
    echo -e "${RED}✗ Integration Tests: FAILED${NC}"
fi

echo ""

# Exit with appropriate code
if [ $MODULE_EXIT -eq 0 ] && [ $INTEGRATION_EXIT -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}⚠ Some tests failed${NC}"
    exit 1
fi
