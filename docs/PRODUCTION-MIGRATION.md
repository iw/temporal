# Production Migration Guide

This document outlines the migration path from our current development-focused Docker build to a production-ready deployment following Temporal's official patterns.

## Current State: Development-Focused Build

Our current DSQL Docker build is optimized for development and testing with these characteristics:

### Advantages
- ✅ Simple single-stage build process
- ✅ Fast iteration for DSQL development
- ✅ Dynamic configuration rendering via templates
- ✅ Comprehensive debugging capabilities
- ✅ Self-contained build pipeline
- ✅ Architecture support (amd64, arm64, arm)

### Trade-offs
- ⚠️ Larger image size (~150-200MB vs ~50-80MB official)
- ⚠️ More dependencies (bash, curl, python3)
- ⚠️ Less production-optimized
- ⚠️ Single binary focus (temporal-server only)
- ⚠️ Custom build pipeline vs official patterns

## Migration Path to Production

### Phase 1: Multi-Stage Build Optimization

**Goal**: Reduce image size and attack surface while maintaining DSQL functionality.

```dockerfile
# Production-ready multi-stage build
FROM temporalio/base-builder:1.14.4 AS temporal-dsql-builder
ARG TEMPORAL_DSQL_VERSION=latest
ARG TARGETARCH

WORKDIR /home/builder

# Copy temporal-dsql source
COPY ./temporal-dsql ./temporal/

# Build DSQL-enabled temporal-server
RUN cd ./temporal && \
    GOOS=linux GOARCH=${TARGETARCH} CGO_ENABLED=0 make temporal-server

FROM temporalio/base-server:1.15.4 AS temporal-dsql-server
ARG TEMPORAL_SHA=unknown

WORKDIR /etc/temporal
ENV TEMPORAL_HOME=/etc/temporal

# Copy DSQL-enabled binary
COPY --from=temporal-dsql-builder /home/builder/temporal/temporal-server /usr/local/bin/

# Copy minimal configuration
COPY ./docker/config/persistence-dsql.template.yaml /etc/temporal/config/
COPY ./docker/render-and-start.sh /etc/temporal/

# Minimal Python for config rendering (consider replacing with Go template)
RUN apk add --no-cache python3

USER temporal
ENTRYPOINT ["/etc/temporal/render-and-start.sh"]
```

**Benefits**:
- Smaller final image (~80-100MB)
- Uses official Temporal base images
- Maintains DSQL functionality
- Better security posture

### Phase 2: Multi-Image Strategy

**Goal**: Separate concerns with different images for different use cases.

#### 2.1 Server Image (Production Runtime)
```dockerfile
# temporal-dsql-server:latest
FROM temporalio/base-server:1.15.4
# Minimal runtime with DSQL support
# No debugging tools, optimized for production
```

#### 2.2 Admin Tools Image
```dockerfile
# temporal-dsql-admin:latest  
FROM temporal-dsql-server:latest
# Add tctl, debugging tools, additional utilities
RUN apk add --no-cache curl bash jq
COPY --from=builder /home/builder/tctl/tctl /usr/local/bin/
```

#### 2.3 Development Image
```dockerfile
# temporal-dsql-dev:latest
FROM temporal-dsql-admin:latest
# Full development environment
# Additional debugging tools, development utilities
```

### Phase 3: Configuration Management Evolution

**Goal**: Move from template-based to more robust configuration management.

#### 3.1 Go-based Configuration Rendering
Replace Python template rendering with Go-based configuration:

```go
// cmd/tools/config-renderer/main.go
package main

import (
    "text/template"
    "os"
    // DSQL-specific configuration logic
)

func main() {
    // Render DSQL configuration from environment variables
    // More robust than Python script
    // Better error handling and validation
}
```

#### 3.2 Helm Chart Integration
```yaml
# helm/temporal-dsql/values.yaml
server:
  image:
    repository: temporal-dsql-server
    tag: "1.0.0"
  
  persistence:
    dsql:
      enabled: true
      cluster: "{{ .Values.dsql.clusterArn }}"
      region: "{{ .Values.dsql.region }}"
```

### Phase 4: CI/CD Pipeline Alignment

**Goal**: Align with Temporal's official build and release processes.

#### 4.1 GitHub Actions Workflow
```yaml
# .github/workflows/docker-build.yml
name: Build DSQL Docker Images

on:
  push:
    branches: [main]
    tags: ['v*']

jobs:
  build:
    strategy:
      matrix:
        image: [server, admin, dev]
        platform: [linux/amd64, linux/arm64]
    
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive
      
      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: .
          file: ./docker/${{ matrix.image }}.Dockerfile
          platforms: ${{ matrix.platform }}
          tags: temporal-dsql-${{ matrix.image }}:${{ github.sha }}
```

#### 4.2 Release Management
- Semantic versioning aligned with temporal-dsql releases
- Automated security scanning
- Multi-architecture builds
- Registry management (ECR, Docker Hub, etc.)

### Phase 5: Production Deployment Patterns

**Goal**: Support enterprise deployment patterns.

#### 5.1 Kubernetes Deployment
```yaml
# k8s/temporal-dsql-server.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: temporal-dsql-server
spec:
  template:
    spec:
      containers:
      - name: temporal-server
        image: temporal-dsql-server:1.0.0
        env:
        - name: TEMPORAL_SQL_HOST
          valueFrom:
            secretKeyRef:
              name: dsql-config
              key: host
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

#### 5.2 Security Hardening
- Non-root user enforcement
- Read-only root filesystem
- Security context constraints
- Network policies
- Pod security standards

#### 5.3 Observability Integration
```yaml
# Prometheus metrics
- name: TEMPORAL_PROMETHEUS_ENDPOINT
  value: "0.0.0.0:9090"

# Structured logging
- name: TEMPORAL_LOG_LEVEL
  value: "info"
- name: TEMPORAL_LOG_FORMAT
  value: "json"

# Distributed tracing
- name: TEMPORAL_OTEL_ENABLED
  value: "true"
```

## Implementation Timeline

### Immediate (Current Development Phase)
- ✅ Maintain current development-focused build
- ✅ Document production migration path
- ✅ Continue DSQL feature development

### Short Term (1-2 months)
- 🎯 Implement Phase 1: Multi-stage build optimization
- 🎯 Create production-ready Dockerfile variants
- 🎯 Set up basic CI/CD pipeline

### Medium Term (3-6 months)
- 🎯 Implement Phase 2: Multi-image strategy
- 🎯 Replace Python config rendering with Go
- 🎯 Create Helm charts for Kubernetes deployment

### Long Term (6+ months)
- 🎯 Full CI/CD pipeline with security scanning
- 🎯 Production deployment patterns and documentation
- 🎯 Performance optimization and monitoring

## Decision Points

### When to Migrate?
- **Stay Development-Focused If**:
  - Primary use case is DSQL research and development
  - Team size is small (1-5 developers)
  - Deployment complexity is low
  - Iteration speed is critical

- **Migrate to Production If**:
  - Planning production DSQL deployments
  - Need enterprise-grade security and compliance
  - Scaling to multiple environments
  - Integration with existing Temporal infrastructure

### Hybrid Approach
Consider maintaining both approaches:
- **Development images**: Current approach for fast iteration
- **Production images**: Official Temporal patterns for deployment
- **Shared components**: DSQL plugin, configuration templates

## Resources and References

- [Temporal Docker Builds Repository](https://github.com/temporalio/docker-builds)
- [Temporal Server Configuration](https://docs.temporal.io/references/configuration)
- [Kubernetes Deployment Guide](https://docs.temporal.io/kb/temporal-server-on-kubernetes)
- [Production Deployment Checklist](https://docs.temporal.io/kb/production-deployment)

## Conclusion

Our current development-focused approach is well-suited for DSQL research and development. The migration path provides a clear roadmap to production-ready deployments when needed, allowing us to maintain development velocity while planning for future production requirements.

The key is to evolve incrementally, adopting production patterns as requirements demand while preserving the simplicity and speed that makes our current approach effective for DSQL development.