# 🌊 Tellus Demo: Your First Earth System Model Workflow

*A hands-on walkthrough for managing climate simulations like a pro*

## 🎯 What You'll Learn
In 10 minutes, you'll create a simulation, set up data locations, and explore files - using both traditional CLI and modern REST API approaches.

---

## 🚀 **Quick Setup**

```bash
# Start in the project directory
cd /path/to/tellus

# Install dependencies
pixi install

# Start the API server (optional - for REST mode)
pixi run api
# ✅ Server starts on http://localhost:1968
```

---

## 🧪 **Workflow 1: Traditional CLI Mode**

### Step 1: Create Your First Simulation
```bash
# Create a new FESOM ocean model simulation
pixi run tellus simulation create \
  --simulation-id "my-fesom-run" \
  --model-id "FESOM2" \
  --attrs experiment=PI \
  --attrs resolution=T127 \
  --attrs description="My first FESOM simulation"

# ✅ Creates: my-fesom-run with metadata
```

### Step 2: Add Storage Locations
```bash
# Add a local development location
pixi run tellus location create \
  --name "dev-local" \
  --kind DISK \
  --host "localhost" \
  --path "/tmp/tellus-data/{model}/{experiment}" \
  --description "Local development storage"

# Add an HPC cluster location  
pixi run tellus location create \
  --name "hpc-cluster" \
  --kind COMPUTE \
  --host "supercomputer.university.edu" \
  --path "/scratch/{username}/runs/{model}/{experiment}" \
  --description "University HPC cluster"

# Add an archive location
pixi run tellus location create \
  --name "long-term-archive" \
  --kind TAPE \
  --host "archive.university.edu" \
  --path "/archive/climate/{model}/{experiment}/{simulation_id}" \
  --description "Long-term tape archive"
```

### Step 3: Connect Simulation to Locations
```bash
# Associate your simulation with storage locations
pixi run tellus simulation add-location my-fesom-run dev-local
pixi run tellus simulation add-location my-fesom-run hpc-cluster  
pixi run tellus simulation add-location my-fesom-run long-term-archive

# ✅ Simulation now knows where its data lives
```

### Step 4: Explore Your Setup
```bash
# List all simulations
pixi run tellus simulation list

# View simulation details
pixi run tellus simulation show my-fesom-run

# List available storage locations
pixi run tellus location list

# Test location connectivity
pixi run tellus location test dev-local
```

---

## 🌐 **Workflow 2: Modern REST API Mode**

Switch to the powerful REST API backend:

```bash
# Enable REST API mode
export TELLUS_CLI_USE_REST_API=true
```

### Step 5: Create Another Simulation (via REST)
```bash
# Same commands, now powered by REST API! ✨
pixi run tellus simulation create \
  --simulation-id "my-icon-run" \
  --model-id "ICON" \
  --attrs experiment=RCP85 \
  --attrs resolution=R2B6 \
  --attrs description="Future climate scenario"

# 🔍 Behind the scenes: CLI → REST API → Response
```

### Step 6: Verify with Direct API Calls
```bash
# Check the API health
curl http://localhost:1968/api/v0a3/health

# List simulations via REST
curl http://localhost:1968/api/v0a3/simulations/ | jq

# Get specific simulation
curl http://localhost:1968/api/v0a3/simulations/my-icon-run | jq

# List storage locations
curl http://localhost:1968/api/v0a3/locations/ | jq
```

---

## 📊 **What You Just Built**

### **Your Data Architecture:**
```
📁 Simulations
├── 🌊 my-fesom-run (FESOM2, PI experiment)  
│   ├── 💻 dev-local → /tmp/tellus-data/FESOM2/PI
│   ├── 🖥️  hpc-cluster → /scratch/{user}/runs/FESOM2/PI  
│   └── 📼 long-term-archive → /archive/climate/FESOM2/PI/my-fesom-run
│
└── 🌍 my-icon-run (ICON, RCP85 experiment)
    └── 📍 (locations to be added...)
```

### **Capabilities You Now Have:**
- ✅ **Simulation Management**: Create, configure, and track climate model runs
- ✅ **Multi-Location Support**: Local dev, HPC clusters, long-term archives  
- ✅ **Template-Based Paths**: Dynamic paths using simulation attributes
- ✅ **Dual Interface**: Traditional CLI + Modern REST API
- ✅ **Metadata Tracking**: Rich attributes and relationships

---

## 🔍 **Explore Further**

```bash
# View all simulation attributes  
pixi run tellus simulation show my-fesom-run --format json

# Add custom attributes
pixi run tellus simulation attr my-fesom-run contact "your.email@university.edu"
pixi run tellus simulation attr my-fesom-run status "running"

# Test location paths get resolved correctly
pixi run tellus location test dev-local --context simulation_id=my-fesom-run

# List files at locations (when they exist)
pixi run tellus simulation files my-fesom-run
```

---

## 🎉 **Congratulations!**

You've just experienced **Tellus** - the modern way to manage Earth System Model data:

- 🏗️ **Clean Architecture**: CLI and REST API working in harmony
- 🌐 **Multi-Location**: From local dev to HPC to archives  
- 🎯 **Context-Aware**: Paths that adapt to your simulation parameters
- 📈 **Scalable**: Ready for small experiments or large model intercomparisons

### **Next Steps:**
- Integrate with your actual model workflows
- Set up automated archiving pipelines  
- Build web dashboards using the REST API
- Scale to multiple research projects

**Welcome to modern climate model data management!** 🌊✨

---
*Created with Tellus - The API-first ESM data management platform*