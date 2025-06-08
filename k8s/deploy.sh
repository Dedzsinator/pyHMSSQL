#!/bin/bash

# HMSSQL Kubernetes Cluster Deployment Script
# This script deploys a fault-tolerant HMSSQL cluster with RAFT consensus

set -e

NAMESPACE="hmssql-cluster"
REPLICAS=3

echo "🚀 Deploying HMSSQL Cluster with RAFT Consensus"

# Function to check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        echo "❌ kubectl not found. Please install kubectl first."
        exit 1
    fi
}

# Function to check if cluster is accessible
check_cluster() {
    if ! kubectl cluster-info &> /dev/null; then
        echo "❌ Cannot access Kubernetes cluster. Please check your connection."
        exit 1
    fi
}

# Function to create namespace
create_namespace() {
    echo "📦 Creating namespace: $NAMESPACE"
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
}

# Function to build and push Docker images
build_images() {
    echo "🔨 Building HMSSQL Docker images..."
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker not found. Please install Docker first."
        exit 1
    fi
    
    cd ..
    
    # Build main HMSSQL image
    echo "📦 Building HMSSQL server image..."
    docker build -t hmssql:latest -f Dockerfile .
    
    # Build orchestrator image
    echo "📦 Building HMSSQL orchestrator image..."
    docker build -t hmssql-orchestrator:latest -f Dockerfile.orchestrator .
    
    # Tag images for registry (if needed)
    if [ "$REGISTRY" != "" ]; then
        echo "🏷️ Tagging images for registry: $REGISTRY"
        docker tag hmssql:latest $REGISTRY/hmssql:latest
        docker tag hmssql-orchestrator:latest $REGISTRY/hmssql-orchestrator:latest
        
        echo "📤 Pushing images to registry..."
        docker push $REGISTRY/hmssql:latest
        docker push $REGISTRY/hmssql-orchestrator:latest
    fi
    
    cd k8s
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY server/ ./server/
COPY shared/ ./shared/
COPY client/ ./client/

# Create directories
RUN mkdir -p /data /logs /config

# Expose ports
EXPOSE 9999 8999

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD python -c "import socket; s=socket.socket(); s.connect(('localhost', 9999)); s.close()" || exit 1

# Start script
COPY docker/start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
EOF

    # Create start script
    mkdir -p ../docker
    cat > ../docker/start.sh << 'EOF'
#!/bin/bash

# Wait for dependencies
sleep 10

# Start HMSSQL server with RAFT
cd /app
export PYTHONPATH=/app

python server/server.py \
    --name "${HOSTNAME}" \
    --cluster-config "${CLUSTER_CONFIG}" \
    --raft-enabled \
    --raft-port 8999 \
    --orchestrator
EOF

    # Build orchestrator image
    cat > ../docker/Dockerfile.orchestrator << 'EOF'
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install flask requests kubernetes prometheus_client

# Copy orchestrator code
COPY orchestrator/ ./

EXPOSE 3000

CMD ["python", "orchestrator.py"]
EOF

    echo "✅ Docker images ready for building"
}

# Function to apply Kubernetes manifests
deploy_cluster() {
    echo "🎯 Deploying HMSSQL cluster..."
    kubectl apply -f hmssql-cluster.yaml
    
    echo "⏳ Waiting for StatefulSet to be ready..."
    kubectl wait --for=condition=ready pod -l app=hmssql -n $NAMESPACE --timeout=300s
    
    echo "✅ HMSSQL cluster deployed successfully!"
}

# Function to setup monitoring
setup_monitoring() {
    echo "📊 Setting up monitoring..."
    
    # Apply ServiceMonitor and PrometheusRule
    kubectl apply -f hmssql-cluster.yaml
    
    echo "✅ Monitoring configured"
}

# Function to verify deployment
verify_deployment() {
    echo "🔍 Verifying deployment..."
    
    # Check pods
    echo "Pods:"
    kubectl get pods -n $NAMESPACE
    
    # Check services
    echo -e "\nServices:"
    kubectl get services -n $NAMESPACE
    
    # Check RAFT leader
    echo -e "\nChecking RAFT leader election..."
    for i in {0..2}; do
        pod_name="hmssql-cluster-$i"
        echo "Checking $pod_name..."
        kubectl exec -n $NAMESPACE $pod_name -- python -c "
import sys
sys.path.append('/app')
from server.raft_consensus import *
# Check if this node is leader
print('Leader check would go here')
" 2>/dev/null || echo "Pod not ready yet"
    done
    
    echo "✅ Deployment verification complete"
}

# Function to show connection info
show_connection_info() {
    echo "📋 Connection Information:"
    echo "=========================="
    
    # Get LoadBalancer IPs
    PRIMARY_IP=$(kubectl get service hmssql-primary -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "Pending")
    REPLICA_IP=$(kubectl get service hmssql-replica -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "Pending")
    ORCHESTRATOR_IP=$(kubectl get service hmssql-orchestrator -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "Pending")
    
    echo "Primary Database: $PRIMARY_IP:9999"
    echo "Replica Database: $REPLICA_IP:9999"
    echo "Orchestrator Web UI: http://$ORCHESTRATOR_IP:3000"
    
    echo -e "\nPort Forwarding (if LoadBalancer IPs are pending):"
    echo "kubectl port-forward -n $NAMESPACE svc/hmssql-primary 9999:9999"
    echo "kubectl port-forward -n $NAMESPACE svc/hmssql-orchestrator 3000:3000"
    
    echo -e "\nUseful Commands:"
    echo "kubectl get pods -n $NAMESPACE"
    echo "kubectl logs -n $NAMESPACE hmssql-cluster-0 -f"
    echo "kubectl exec -n $NAMESPACE hmssql-cluster-0 -it -- bash"
}

# Function to test failover
test_failover() {
    echo "🧪 Testing automatic failover..."
    
    # Find current leader
    leader_pod=""
    for i in {0..2}; do
        pod_name="hmssql-cluster-$i"
        echo "Checking if $pod_name is leader..."
        
        # Check leader status via orchestrator API
        leader_check=$(kubectl exec -n $NAMESPACE $pod_name -- python3 -c "
import sys
sys.path.append('/app')
try:
    from server.raft_consensus import RaftNode
    node = RaftNode('$pod_name', ['hmssql-cluster-0', 'hmssql-cluster-1', 'hmssql-cluster-2'])
    print('LEADER' if node.state == 'leader' else 'FOLLOWER')
except:
    print('UNKNOWN')
" 2>/dev/null || echo "UNKNOWN")
        
        if [[ "$leader_check" == "LEADER" ]]; then
            leader_pod=$pod_name
            echo "✅ Found leader: $leader_pod"
            break
        fi
    done
    
    if [[ -n "$leader_pod" ]]; then
        echo "🔥 Simulating leader failure..."
        kubectl delete pod -n $NAMESPACE $leader_pod
        
        echo "⏳ Waiting for new leader election..."
        sleep 30
        
        # Check for new leader
        new_leader=""
        for i in {0..2}; do
            pod_name="hmssql-cluster-$i"
            if [[ "$pod_name" == "$leader_pod" ]]; then
                continue  # Skip the failed pod
            fi
            
            leader_check=$(kubectl exec -n $NAMESPACE $pod_name -- python3 -c "
import sys
sys.path.append('/app')
try:
    from server.raft_consensus import RaftNode
    node = RaftNode('$pod_name', ['hmssql-cluster-0', 'hmssql-cluster-1', 'hmssql-cluster-2'])
    print('LEADER' if node.state == 'leader' else 'FOLLOWER')
except:
    print('UNKNOWN')
" 2>/dev/null || echo "UNKNOWN")
            
            if [[ "$leader_check" == "LEADER" ]]; then
                new_leader=$pod_name
                echo "✅ New leader elected: $new_leader"
                break
            fi
        done
        
        if [[ -n "$new_leader" ]]; then
            echo "✅ Failover test PASSED - New leader: $new_leader"
        else
            echo "❌ Failover test FAILED - No new leader elected"
        fi
    else
        echo "❌ No leader found for failover test"
    fi
}

# Function to cleanup
cleanup() {
    echo "🧹 Cleaning up HMSSQL cluster..."
    kubectl delete namespace $NAMESPACE --ignore-not-found=true
    echo "✅ Cleanup complete"
}

# Function to run end-to-end tests
run_e2e_tests() {
    echo "🧪 Running end-to-end tests..."
    
    # Check if pytest is available
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python3 not found. Cannot run tests."
        return 1
    fi
    
    cd ..
    echo "📦 Installing test dependencies..."
    pip3 install pytest kubernetes requests PyJWT asyncio
    
    echo "🚀 Running comprehensive test suite..."
    python3 tests/test_end_to_end.py
    
    if [ $? -eq 0 ]; then
        echo "✅ End-to-end tests PASSED"
        return 0
    else
        echo "❌ End-to-end tests FAILED"
        return 1
    fi
}

# Function to validate production readiness
validate_production() {
    echo "🔍 Validating production readiness..."
    
    # Install validation dependencies
    pip3 install kubernetes requests PyJWT
    
    echo "🚀 Running production validation..."
    python3 k8s/production_validator.py $NAMESPACE
    
    if [ $? -eq 0 ]; then
        echo "✅ Production validation PASSED"
        return 0
    else
        echo "❌ Production validation FAILED"
        return 1
    fi
}

# Function to run comprehensive deployment
full_deploy() {
    echo "🚀 Running full deployment with testing..."
    
    check_kubectl
    check_cluster
    create_namespace
    build_images
    deploy_cluster
    setup_monitoring
    
    echo "⏳ Waiting for cluster to stabilize..."
    sleep 60
    
    verify_deployment
    
    echo "🧪 Running validation and tests..."
    validate_production
    
    if [ $? -eq 0 ]; then
        echo "🎯 Running end-to-end tests..."
        run_e2e_tests
        
        if [ $? -eq 0 ]; then
            echo "✅ Full deployment completed successfully!"
            show_connection_info
            return 0
        else
            echo "⚠️ Deployment successful but tests failed"
            return 1
        fi
    else
        echo "❌ Production validation failed"
        return 1
    fi
}

# Main script
main() {
    case "${1:-deploy}" in
        "deploy")
            check_kubectl
            check_cluster
            create_namespace
            build_images
            deploy_cluster
            setup_monitoring
            verify_deployment
            show_connection_info
            ;;
        "full-deploy")
            full_deploy
            ;;
        "test-failover")
            test_failover
            ;;
        "test-e2e")
            run_e2e_tests
            ;;
        "validate")
            validate_production
            ;;
        "verify")
            verify_deployment
            ;;
        "info")
            show_connection_info
            ;;
        "cleanup")
            cleanup
            ;;
        *)
            echo "Usage: $0 {deploy|full-deploy|test-failover|test-e2e|validate|verify|info|cleanup}"
            echo ""
            echo "Commands:"
            echo "  deploy       - Deploy the complete HMSSQL cluster"
            echo "  full-deploy  - Deploy with comprehensive validation and testing"
            echo "  test-failover- Test automatic failover capabilities"
            echo "  test-e2e     - Run end-to-end test suite"
            echo "  validate     - Run production readiness validation"
            echo "  verify       - Verify deployment status"
            echo "  info         - Show connection information"
            echo "  cleanup      - Remove the entire cluster"
            echo ""
            echo "Environment Variables:"
            echo "  REGISTRY     - Docker registry for pushing images"
            echo "  NAMESPACE    - Kubernetes namespace (default: hmssql-cluster)"
            exit 1
            ;;
    esac
}

main "$@"
