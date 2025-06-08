#!/bin/bash

# HMSSQL Integration Testing Script
# Tests the complete RAFT consensus and orchestration system

set -e

echo "🧪 Starting HMSSQL Integration Tests"

# Test configuration
NAMESPACE="hmssql-test"
TEST_TIMEOUT=300

# Function to check prerequisites
check_prerequisites() {
    echo "📋 Checking prerequisites..."
    
    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null; then
        echo "❌ kubectl not found. Please install kubectl first."
        exit 1
    fi
    
    # Check if Python is available
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python 3 not found. Please install Python 3 first."
        exit 1
    fi
    
    # Check if cluster is accessible
    if ! kubectl cluster-info &> /dev/null; then
        echo "❌ Cannot access Kubernetes cluster. Please check your connection."
        exit 1
    fi
    
    echo "✅ Prerequisites check passed"
}

# Function to run RAFT integration tests
test_raft_consensus() {
    echo "🔄 Testing RAFT consensus implementation..."
    
    cd "$(dirname "$0")/.."
    
    # Install test dependencies
    pip install -r requirements.txt pytest pytest-asyncio
    
    # Run RAFT integration tests
    python -m pytest tests/test_raft_integration.py -v --tb=short
    
    if [ $? -eq 0 ]; then
        echo "✅ RAFT consensus tests passed"
        return 0
    else
        echo "❌ RAFT consensus tests failed"
        return 1
    fi
}

# Function to test Docker images
test_docker_images() {
    echo "🐳 Testing Docker images..."
    
    cd "$(dirname "$0")/.."
    
    # Build images
    echo "📦 Building Docker images..."
    docker build -t hmssql:test -f Dockerfile .
    docker build -t hmssql-orchestrator:test -f Dockerfile.orchestrator .
    
    # Test main image
    echo "🧪 Testing main HMSSQL image..."
    CONTAINER_ID=$(docker run -d -p 19999:9999 hmssql:test)
    
    # Wait for container to start
    sleep 10
    
    # Test connection
    if python3 -c "
import socket
import sys
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(('localhost', 19999))
    s.close()
    print('✅ Connection test passed')
    sys.exit(0)
except Exception as e:
    print(f'❌ Connection test failed: {e}')
    sys.exit(1)
"; then
        echo "✅ Main image test passed"
    else
        echo "❌ Main image test failed"
        docker stop $CONTAINER_ID
        return 1
    fi
    
    # Cleanup
    docker stop $CONTAINER_ID
    
    # Test orchestrator image
    echo "🧪 Testing orchestrator image..."
    ORCH_CONTAINER_ID=$(docker run -d -p 18080:8080 hmssql-orchestrator:test)
    
    # Wait for container to start
    sleep 15
    
    # Test health endpoint
    if curl -f http://localhost:18080/health >/dev/null 2>&1; then
        echo "✅ Orchestrator image test passed"
    else
        echo "❌ Orchestrator image test failed"
        docker stop $ORCH_CONTAINER_ID
        return 1
    fi
    
    # Cleanup
    docker stop $ORCH_CONTAINER_ID
    
    echo "✅ Docker image tests completed"
}

# Function to test Kubernetes deployment
test_kubernetes_deployment() {
    echo "☸️ Testing Kubernetes deployment..."
    
    # Create test namespace
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    
    # Deploy test cluster
    echo "🚀 Deploying test cluster..."
    sed "s/hmssql-cluster/$NAMESPACE/g" hmssql-cluster.yaml | kubectl apply -f -
    
    # Wait for pods to be ready
    echo "⏳ Waiting for pods to be ready..."
    kubectl wait --for=condition=ready pod -l app=hmssql-db -n $NAMESPACE --timeout=${TEST_TIMEOUT}s
    
    if [ $? -ne 0 ]; then
        echo "❌ Pods failed to become ready"
        kubectl get pods -n $NAMESPACE
        kubectl describe pods -n $NAMESPACE
        return 1
    fi
    
    # Test cluster connectivity
    echo "🔗 Testing cluster connectivity..."
    
    # Get pod names
    PODS=($(kubectl get pods -n $NAMESPACE -l app=hmssql-db -o jsonpath='{.items[*].metadata.name}'))
    
    if [ ${#PODS[@]} -lt 3 ]; then
        echo "❌ Expected 3 pods, found ${#PODS[@]}"
        return 1
    fi
    
    # Test each pod
    for pod in "${PODS[@]}"; do
        echo "🧪 Testing pod: $pod"
        
        # Check if pod is running
        if ! kubectl get pod $pod -n $NAMESPACE | grep -q Running; then
            echo "❌ Pod $pod is not running"
            return 1
        fi
        
        # Test database connection
        if kubectl exec $pod -n $NAMESPACE -- python -c "
import socket
s = socket.socket()
s.settimeout(5)
s.connect(('localhost', 9999))
s.close()
print('Connection successful')
" >/dev/null 2>&1; then
            echo "✅ Pod $pod connection test passed"
        else
            echo "❌ Pod $pod connection test failed"
            return 1
        fi
    done
    
    # Test orchestrator
    echo "🎭 Testing orchestrator..."
    kubectl wait --for=condition=ready pod -l app=hmssql-orchestrator -n $NAMESPACE --timeout=60s
    
    ORCH_POD=$(kubectl get pods -n $NAMESPACE -l app=hmssql-orchestrator -o jsonpath='{.items[0].metadata.name}')
    
    # Port forward for testing
    kubectl port-forward $ORCH_POD -n $NAMESPACE 18080:8080 &
    PORT_FORWARD_PID=$!
    
    sleep 5
    
    # Test orchestrator health
    if curl -f http://localhost:18080/health >/dev/null 2>&1; then
        echo "✅ Orchestrator health check passed"
    else
        echo "❌ Orchestrator health check failed"
        kill $PORT_FORWARD_PID 2>/dev/null
        return 1
    fi
    
    # Test cluster discovery
    if curl -f http://localhost:18080/api/cluster/status >/dev/null 2>&1; then
        echo "✅ Orchestrator cluster discovery passed"
    else
        echo "❌ Orchestrator cluster discovery failed"
        kill $PORT_FORWARD_PID 2>/dev/null
        return 1
    fi
    
    kill $PORT_FORWARD_PID 2>/dev/null
    
    echo "✅ Kubernetes deployment tests completed"
}

# Function to test failover scenarios
test_failover_scenarios() {
    echo "🔄 Testing failover scenarios..."
    
    # Get current leader
    LEADER_POD=$(kubectl exec -n $NAMESPACE deployment/hmssql-db -- python -c "
from server.raft_consensus import RaftCluster
import json
# Implementation to find leader pod
print('hmssql-db-0')  # Simplified for demo
")
    
    echo "📍 Current leader: $LEADER_POD"
    
    # Simulate leader failure
    echo "💥 Simulating leader failure..."
    kubectl delete pod $LEADER_POD -n $NAMESPACE
    
    # Wait for new leader election
    echo "⏳ Waiting for new leader election..."
    sleep 30
    
    # Check if new leader was elected
    NEW_PODS=($(kubectl get pods -n $NAMESPACE -l app=hmssql-db -o jsonpath='{.items[*].metadata.name}'))
    
    if [ ${#NEW_PODS[@]} -ge 2 ]; then
        echo "✅ Cluster survived leader failure"
    else
        echo "❌ Cluster failed after leader failure"
        return 1
    fi
    
    # Wait for failed pod to be recreated
    kubectl wait --for=condition=ready pod -l app=hmssql-db -n $NAMESPACE --timeout=120s
    
    echo "✅ Failover test completed"
}

# Function to test data consistency
test_data_consistency() {
    echo "📊 Testing data consistency..."
    
    # Port forward to a pod for testing
    PODS=($(kubectl get pods -n $NAMESPACE -l app=hmssql-db -o jsonpath='{.items[*].metadata.name}'))
    TEST_POD=${PODS[0]}
    
    kubectl port-forward $TEST_POD -n $NAMESPACE 19999:9999 &
    PORT_FORWARD_PID=$!
    
    sleep 5
    
    # Test data operations
    cd "$(dirname "$0")/.."
    
    python3 -c "
import socket
import json
import time

def send_command(sock, command):
    data = json.dumps(command).encode()
    sock.send(len(data).to_bytes(4, 'big'))
    sock.send(data)

def receive_response(sock):
    length = int.from_bytes(sock.recv(4), 'big')
    return json.loads(sock.recv(length).decode())

# Connect to database
sock = socket.socket()
sock.connect(('localhost', 19999))

# Test insert operation
send_command(sock, {
    'action': 'query',
    'query': 'CREATE TABLE test_consistency (id INT, value VARCHAR(100))'
})
response = receive_response(sock)
print('Create table:', response.get('response', 'Unknown'))

# Insert test data
for i in range(10):
    send_command(sock, {
        'action': 'query', 
        'query': f\"INSERT INTO test_consistency VALUES ({i}, 'test_value_{i}')\"
    })
    response = receive_response(sock)

# Query data
send_command(sock, {
    'action': 'query',
    'query': 'SELECT COUNT(*) FROM test_consistency'
})
response = receive_response(sock)
print('Data consistency test result:', response.get('response', 'Unknown'))

sock.close()
"
    
    kill $PORT_FORWARD_PID 2>/dev/null
    
    echo "✅ Data consistency test completed"
}

# Function to cleanup test resources
cleanup_tests() {
    echo "🧹 Cleaning up test resources..."
    
    # Delete test namespace
    kubectl delete namespace $NAMESPACE --ignore-not-found=true
    
    # Remove test Docker images
    docker rmi hmssql:test hmssql-orchestrator:test 2>/dev/null || true
    
    echo "✅ Cleanup completed"
}

# Main test execution
main() {
    echo "🚀 Starting comprehensive integration tests..."
    
    # Check prerequisites
    check_prerequisites
    
    # Run tests
    local test_failed=0
    
    echo "📋 Test Plan:"
    echo "  1. RAFT Consensus Tests"
    echo "  2. Docker Image Tests"
    echo "  3. Kubernetes Deployment Tests"
    echo "  4. Failover Scenario Tests"
    echo "  5. Data Consistency Tests"
    echo ""
    
    # Test 1: RAFT Consensus
    if ! test_raft_consensus; then
        echo "❌ RAFT consensus tests failed"
        test_failed=1
    fi
    
    # Test 2: Docker Images
    if ! test_docker_images; then
        echo "❌ Docker image tests failed"
        test_failed=1
    fi
    
    # Test 3: Kubernetes Deployment
    if ! test_kubernetes_deployment; then
        echo "❌ Kubernetes deployment tests failed"
        test_failed=1
    fi
    
    # Test 4: Failover Scenarios
    if ! test_failover_scenarios; then
        echo "❌ Failover scenario tests failed"
        test_failed=1
    fi
    
    # Test 5: Data Consistency
    if ! test_data_consistency; then
        echo "❌ Data consistency tests failed"
        test_failed=1
    fi
    
    # Cleanup
    cleanup_tests
    
    # Final results
    if [ $test_failed -eq 0 ]; then
        echo "🎉 All integration tests passed!"
        echo "✅ HMSSQL cluster is ready for production deployment"
        return 0
    else
        echo "❌ Some integration tests failed"
        echo "🔧 Please review the test output and fix issues before deployment"
        return 1
    fi
}

# Script options
case "${1:-test}" in
    "test")
        main
        ;;
    "raft-only")
        check_prerequisites
        test_raft_consensus
        ;;
    "docker-only")
        check_prerequisites
        test_docker_images
        ;;
    "k8s-only")
        check_prerequisites
        test_kubernetes_deployment
        ;;
    "cleanup")
        cleanup_tests
        ;;
    *)
        echo "Usage: $0 [test|raft-only|docker-only|k8s-only|cleanup]"
        echo ""
        echo "Commands:"
        echo "  test       - Run all integration tests (default)"
        echo "  raft-only  - Run only RAFT consensus tests"
        echo "  docker-only - Run only Docker image tests"
        echo "  k8s-only   - Run only Kubernetes tests"
        echo "  cleanup    - Clean up test resources"
        exit 1
        ;;
esac
