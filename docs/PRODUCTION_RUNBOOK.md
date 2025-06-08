# pyHMSSQL Production Deployment Guide

## Table of Contents

1. [Pre-Deployment Checklist](#pre-deployment-checklist)
2. [Deployment Procedure](#deployment-procedure)
3. [Post-Deployment Validation](#post-deployment-validation)
4. [Operational Procedures](#operational-procedures)
5. [Monitoring and Alerting](#monitoring-and-alerting)
6. [Disaster Recovery](#disaster-recovery)
7. [Security Operations](#security-operations)
8. [Troubleshooting](#troubleshooting)
9. [Performance Tuning](#performance-tuning)
10. [Maintenance Procedures](#maintenance-procedures)

## Pre-Deployment Checklist

### Infrastructure Requirements

- [ ] Kubernetes cluster version 1.20+
- [ ] At least 3 worker nodes for HA
- [ ] Minimum 4 CPU cores, 8GB RAM per node
- [ ] SSD storage class available
- [ ] Network policies support enabled
- [ ] RBAC enabled
- [ ] Ingress controller configured
- [ ] Container registry access

### Security Prerequisites

- [ ] TLS certificates prepared or cert-manager installed
- [ ] Network security policies reviewed
- [ ] RBAC permissions configured
- [ ] Service accounts created
- [ ] Secrets management strategy defined
- [ ] Audit logging destination configured

### Monitoring Prerequisites

- [ ] Prometheus operator installed
- [ ] Grafana configured
- [ ] AlertManager configured
- [ ] Log aggregation system available
- [ ] Notification channels configured

## Deployment Procedure

### Step 1: Environment Preparation

```bash
# Set environment variables
export NAMESPACE="hmssql-cluster"
export REGISTRY="your-registry.com"
export CLUSTER_SIZE=3

# Verify cluster access
kubectl cluster-info
kubectl get nodes
```

### Step 2: Deploy Security Policies

```bash
# Apply security policies first
kubectl apply -f k8s/security-policies.yaml

# Verify network policies
kubectl get networkpolicies -n $NAMESPACE
```

### Step 3: Deploy HMSSQL Cluster

```bash
# Option 1: Basic deployment
./k8s/deploy.sh deploy

# Option 2: Full deployment with validation
./k8s/deploy.sh full-deploy
```

### Step 4: Validate Deployment

```bash
# Run production validation
./k8s/deploy.sh validate

# Run end-to-end tests
./k8s/deploy.sh test-e2e

# Test failover capabilities
./k8s/deploy.sh test-failover
```

## Post-Deployment Validation

### Cluster Health Checks

```bash
# Check pod status
kubectl get pods -n $NAMESPACE -o wide

# Check services
kubectl get services -n $NAMESPACE

# Check StatefulSet
kubectl get statefulsets -n $NAMESPACE

# Check PersistentVolumes
kubectl get pv,pvc -n $NAMESPACE
```

### RAFT Consensus Validation

```bash
# Check leader election
for i in {0..2}; do
  echo "Checking hmssql-cluster-$i:"
  kubectl exec -n $NAMESPACE hmssql-cluster-$i -- \
    python3 -c "from server.raft_consensus import *; print(get_cluster_status())"
done
```

### Database Connectivity

```bash
# Port forward to primary service
kubectl port-forward -n $NAMESPACE svc/hmssql-primary 9999:9999 &

# Test connection
python3 -c "
import socket
s = socket.socket()
s.connect(('localhost', 9999))
s.send(b'SELECT 1')
print(s.recv(1024))
s.close()
"
```

### Security Validation

```bash
# Check TLS certificates
kubectl get secrets -n $NAMESPACE | grep tls

# Verify network policies
kubectl describe networkpolicy -n $NAMESPACE

# Check RBAC
kubectl get roles,rolebindings -n $NAMESPACE
```

## Operational Procedures

### Daily Operations

1. **Health Monitoring**
   ```bash
   # Check cluster health
   kubectl get pods -n $NAMESPACE --show-labels
   
   # Check resource usage
   kubectl top pods -n $NAMESPACE
   kubectl top nodes
   ```

2. **Log Review**
   ```bash
   # Check application logs
   kubectl logs -n $NAMESPACE -l app=hmssql --tail=100
   
   # Check orchestrator logs
   kubectl logs -n $NAMESPACE -l app=hmssql-orchestrator --tail=100
   ```

3. **Backup Verification**
   ```bash
   # Check backup jobs
   kubectl get cronjobs -n $NAMESPACE
   
   # Verify recent backups
   kubectl logs -n $NAMESPACE job/hmssql-backup-$(date +%Y%m%d)
   ```

### Weekly Operations

1. **Performance Review**
   - Review Grafana dashboards
   - Check query performance metrics
   - Analyze resource utilization trends

2. **Security Review**
   - Review audit logs
   - Check certificate expiry dates
   - Validate access patterns

3. **Backup Testing**
   - Test backup restoration process
   - Verify backup integrity
   - Update disaster recovery procedures

### Monthly Operations

1. **Capacity Planning**
   - Review growth trends
   - Plan resource scaling
   - Update resource quotas

2. **Security Updates**
   - Update container images
   - Review security policies
   - Rotate certificates if needed

3. **Disaster Recovery Testing**
   - Conduct failover tests
   - Test backup/restore procedures
   - Update runbooks

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Cluster Health**
   - Pod availability
   - StatefulSet replicas ready
   - Service endpoint health

2. **RAFT Consensus**
   - Leader election time
   - Log replication latency
   - Node connectivity

3. **Database Performance**
   - Query response time
   - Throughput (QPS)
   - Connection count
   - Resource utilization

4. **Security**
   - Authentication failures
   - Suspicious access patterns
   - Certificate expiry

### Critical Alerts

1. **High Priority**
   - Cluster leader lost
   - Multiple pod failures
   - Security breach detected
   - Backup failure

2. **Medium Priority**
   - High resource usage
   - Slow query performance
   - Certificate expiring soon

3. **Low Priority**
   - Minor configuration drift
   - Informational security events

### Dashboard URLs

- Primary Dashboard: `http://hmssql-orchestrator:3000`
- Grafana: `http://grafana:3000/dashboards`
- Prometheus: `http://prometheus:9090`

## Disaster Recovery

### Scenario 1: Single Node Failure

```bash
# Check affected pods
kubectl get pods -n $NAMESPACE | grep -v Running

# Wait for automatic recovery
kubectl wait --for=condition=ready pod -l app=hmssql -n $NAMESPACE --timeout=300s

# Verify new leader election
kubectl logs -n $NAMESPACE -l app=hmssql | grep "leader elected"
```

### Scenario 2: Multiple Node Failure

```bash
# Check cluster quorum
kubectl get pods -n $NAMESPACE -o wide

# If quorum lost, manual intervention required
# 1. Identify surviving nodes
# 2. Force leader election on majority nodes
# 3. Restore data from backups if needed
```

### Scenario 3: Data Corruption

```bash
# Stop affected pods
kubectl scale statefulset hmssql-cluster --replicas=0 -n $NAMESPACE

# Restore from backup
kubectl apply -f backup-restore-job.yaml

# Restart cluster
kubectl scale statefulset hmssql-cluster --replicas=3 -n $NAMESPACE
```

### Scenario 4: Complete Cluster Loss

```bash
# Deploy new cluster
./k8s/deploy.sh deploy

# Restore data from external backup
kubectl apply -f disaster-recovery-job.yaml

# Validate data integrity
python3 scripts/validate_data_integrity.py
```

## Security Operations

### Certificate Management

```bash
# Check certificate expiry
kubectl get secrets -n $NAMESPACE -o custom-columns=NAME:.metadata.name,EXPIRES:.data.expiry

# Rotate certificates
kubectl delete secret hmssql-tls-secret -n $NAMESPACE
kubectl apply -f k8s/security-policies.yaml
```

### Access Control

```bash
# Review user access
kubectl get rolebindings -n $NAMESPACE -o yaml

# Audit access logs
kubectl logs -n $NAMESPACE -l app=hmssql | grep "auth"
```

### Security Scanning

```bash
# Scan for vulnerabilities
kubectl run security-scanner --rm -it --image=aquasec/trivy:latest -- \
  image hmssql:latest

# Check pod security policies
kubectl get psp,podsecuritypolicy
```

## Troubleshooting

### Common Issues

1. **Pod Stuck in Pending**
   ```bash
   kubectl describe pod <pod-name> -n $NAMESPACE
   # Check: resource constraints, node affinity, PVC binding
   ```

2. **RAFT Leader Election Failure**
   ```bash
   kubectl logs -n $NAMESPACE <pod-name> | grep -i raft
   # Check: network connectivity, time synchronization
   ```

3. **Database Connection Issues**
   ```bash
   kubectl port-forward -n $NAMESPACE svc/hmssql-primary 9999:9999
   telnet localhost 9999
   # Check: service endpoints, network policies
   ```

4. **Performance Issues**
   ```bash
   kubectl top pods -n $NAMESPACE
   kubectl describe node <node-name>
   # Check: resource limits, storage I/O, network latency
   ```

### Log Analysis

```bash
# Application logs
kubectl logs -n $NAMESPACE -l app=hmssql --tail=1000 | grep ERROR

# System events
kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp'

# RAFT consensus logs
kubectl logs -n $NAMESPACE <pod-name> | grep -i "raft\|leader\|election"
```

### Debug Commands

```bash
# Enter pod for debugging
kubectl exec -it -n $NAMESPACE hmssql-cluster-0 -- /bin/bash

# Check network connectivity
kubectl exec -n $NAMESPACE hmssql-cluster-0 -- \
  nc -zv hmssql-cluster-1 8999

# Test database operations
kubectl exec -n $NAMESPACE hmssql-cluster-0 -- \
  python3 -c "from server.database import *; test_connection()"
```

## Performance Tuning

### Resource Optimization

```bash
# Update resource limits
kubectl patch statefulset hmssql-cluster -n $NAMESPACE -p '{
  "spec": {
    "template": {
      "spec": {
        "containers": [{
          "name": "hmssql",
          "resources": {
            "requests": {"cpu": "1000m", "memory": "2Gi"},
            "limits": {"cpu": "2000m", "memory": "4Gi"}
          }
        }]
      }
    }
  }
}'
```

### Storage Optimization

```bash
# Check storage performance
kubectl exec -n $NAMESPACE hmssql-cluster-0 -- \
  fio --name=test --rw=randwrite --size=1G --bs=4k --numjobs=1

# Optimize storage class
kubectl patch storageclass fast-ssd -p '{
  "parameters": {
    "type": "gp3",
    "iops": "3000",
    "throughput": "125"
  }
}'
```

### Network Optimization

```bash
# Update anti-affinity rules
kubectl patch statefulset hmssql-cluster -n $NAMESPACE -p '{
  "spec": {
    "template": {
      "spec": {
        "affinity": {
          "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [{
              "labelSelector": {
                "matchExpressions": [{
                  "key": "app",
                  "operator": "In",
                  "values": ["hmssql"]
                }]
              },
              "topologyKey": "kubernetes.io/hostname"
            }]
          }
        }
      }
    }
  }
}'
```

## Maintenance Procedures

### Planned Maintenance

1. **Pre-maintenance**
   ```bash
   # Create backup
   kubectl create job backup-maintenance --from=cronjob/hmssql-backup -n $NAMESPACE
   
   # Verify backup completion
   kubectl wait --for=condition=complete job/backup-maintenance -n $NAMESPACE
   ```

2. **Rolling Updates**
   ```bash
   # Update container image
   kubectl set image statefulset/hmssql-cluster hmssql=hmssql:new-version -n $NAMESPACE
   
   # Monitor rollout
   kubectl rollout status statefulset/hmssql-cluster -n $NAMESPACE
   ```

3. **Post-maintenance**
   ```bash
   # Verify cluster health
   ./k8s/deploy.sh verify
   
   # Run health checks
   python3 scripts/health_check.py
   ```

### Emergency Procedures

1. **Emergency Shutdown**
   ```bash
   # Scale down cluster
   kubectl scale statefulset hmssql-cluster --replicas=0 -n $NAMESPACE
   
   # Wait for graceful shutdown
   kubectl wait --for=delete pod -l app=hmssql -n $NAMESPACE --timeout=300s
   ```

2. **Emergency Recovery**
   ```bash
   # Deploy minimal cluster
   kubectl scale statefulset hmssql-cluster --replicas=1 -n $NAMESPACE
   
   # Force single-node mode
   kubectl exec -n $NAMESPACE hmssql-cluster-0 -- \
     python3 -c "from server.raft_consensus import *; force_single_node_mode()"
   ```

## Contact Information

- **Primary Oncall**: ops-team@company.com
- **Database Team**: dba-team@company.com
- **Security Team**: security-team@company.com
- **Escalation**: engineering-manager@company.com

## Additional Resources

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [RAFT Consensus Algorithm](https://raft.github.io/)
- [pyHMSSQL GitHub Repository](https://github.com/your-org/pyHMSSQL)
- [Internal Wiki](https://wiki.company.com/pyHMSSQL)

---

**Last Updated**: $(date)
**Version**: 1.0
**Maintained By**: Database Operations Team
