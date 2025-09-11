# 🌊 Tellus Demo: Your First Earth System Model Workflow

*A hands-on walkthrough using the real CLI commands that actually exist*

## 🎯 What You'll Learn
In 10 minutes, you'll create simulations, set up storage locations, and explore the actual Tellus workflow using both traditional CLI and modern REST API approaches.

---

## 🚀 **Quick Setup**

```bash
# Clone the repository
git clone https://github.com/pgierz/tellus.git

# Enter the directory, and ensure you are on the rest branch
cd tellus
git checkout rest

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
pixi run tellus simulation create my-fesom-run --model-id FESOM2

# ✅ Creates: my-fesom-run simulation
```

### Step 2: Add Storage Locations
```bash
# Add a local development location
pixi run tellus location create dev-local --protocol=file --kind=disk --path=/tmp/dev-storage

# Add an HPC cluster location  
pixi run tellus location create hpc-cluster --protocol=ssh --kind=compute --host=login.hpc.example.com --path=/work/username/simulations

# ✅ Creates locations with all required parameters specified directly
```

### Step 3: Connect Simulation to Locations
```bash
# Associate your simulation with storage locations
pixi run tellus simulation location add my-fesom-run dev-local
pixi run tellus simulation location add my-fesom-run hpc-cluster

# ✅ Simulation now knows where its data can be stored
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

# List simulation-location associations
pixi run tellus simulation location list my-fesom-run
```

### Step 5: Work with Simulation Data
```bash
# List contents at a simulation location
pixi run tellus simulation location ls my-fesom-run dev-local

# Upload a file to the simulation location
# pixi run tellus simulation location put my-fesom-run dev-local local-file.txt

# Download a file from the simulation location  
# pixi run tellus simulation location get my-fesom-run dev-local remote-file.txt

# ✅ Tellus provides git-like operations for simulation data
```

---

## 🌐 **Workflow 2: Modern REST API Mode**

Switch to the powerful REST API backend:

```bash
# Enable REST API mode
export TELLUS_CLI_USE_REST_API=true
```

### Step 6: Create Another Simulation (via REST)
```bash
# Same commands, now powered by REST API! ✨
pixi run tellus simulation create my-icon-run --model-id ICON

# 🔍 Behind the scenes: CLI → REST API → Response
```

### Step 7: Verify with Direct API Calls
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
├── 🌊 my-fesom-run (FESOM2)
│   ├── 💻 dev-local (associated)
│   └── 🖥️ hpc-cluster (associated)
│
└── 🌍 my-icon-run (ICON)
    └── 📍 (ready for location associations)

📍 Locations
├── dev-local (local development)
└── hpc-cluster (HPC compute)
```

### **Real Commands You Can Use:**
- ✅ **Simulation Management**: `create`, `list`, `show`, `update`, `delete`
- ✅ **Location Management**: `create`, `list`, `show`, `test`, `update`, `delete`
- ✅ **Location Associations**: `simulation location add/remove/list`
- ✅ **File Operations**: `simulation location ls/get/put/mget/mput`
- ✅ **Dual Interface**: Traditional CLI + Modern REST API

---

## 🔍 **Explore Further**

```bash
# Manage simulation attributes
pixi run tellus simulation attrs my-fesom-run

# Edit simulation metadata
pixi run tellus simulation edit my-fesom-run

# Update location association settings
pixi run tellus simulation location update my-fesom-run dev-local

# Work with simulation files
pixi run tellus simulation files my-fesom-run

# Manage workflows
pixi run tellus simulation workflow my-fesom-run

# Remove location association
pixi run tellus simulation location remove my-fesom-run dev-local
```

---

## 🎉 **Congratulations!**

You've just experienced the **real Tellus CLI** - the modern way to manage Earth System Model data:

- 🏗️ **Clean Architecture**: CLI and REST API working in harmony
- 🌐 **Multi-Location**: Support for various storage backends
- 🎯 **Git-like Operations**: Familiar file management commands
- 📈 **Scalable**: Ready for small experiments or large model intercomparisons

### **Key Features Demonstrated:**
- **Direct Parameter Commands**: All operations work with explicit parameters
- **Location Associations**: Connect simulations to storage locations
- **File Operations**: Upload, download, and list files at remote locations
- **REST API Integration**: Same functionality via HTTP endpoints
- **Dual Mode**: Switch between direct service calls and REST API backend

### **Known Issues:**
- **Interactive Wizards**: Currently affected by VSplit compatibility issue ([Issue #54](https://github.com/pgierz/tellus/issues/54)). Use direct parameter commands as shown above.

### **Next Steps:**
- Upload actual simulation data using `put` commands
- Explore the REST API endpoints directly
- Set up automated workflows with the API
- Try the locations with real remote hosts (SSH keys required)

**Welcome to modern climate model data management!** 🌊✨

---
*Created with Tellus - The API-first ESM data management platform*
