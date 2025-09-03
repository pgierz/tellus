# Tellus

> "The mouthpiece of AWI's TerraAI"

Tellus is a domain-driven Python framework that abstracts climate model simulations into semantic, AI-understandable objects. From solving today's data chaos to enabling tomorrow's AI-native planetary modeling.

![Tellus Architecture](docs/architecture-overview.png)

## 🌍 Vision

**Today**: Organize and discover your scattered climate simulation data across multiple storage locations  
**Tomorrow**: AI assistant that understands climate science semantically  
**Future**: Universal framework for AI-native planetary modeling

## ⚡ Quick Start

```python
import tellus

# Connect to your storage locations
tellus.add_location("levante_scratch", 
                   protocol="sftp", 
                   host="levante.dkrz.de",
                   path="/scratch/a270077/experiments")

tellus.add_location("local_archive", 
                   protocol="file",
                   path="/Users/pgierz/climate_data")

# Register your simulation
arctic_sim = SimulationEntity(
    simulation_id="CMIP6_historical_r1i1p1f1",
    model_id="AWI-CM-1-1-MR",
    attrs={
        "experiment": "historical", 
        "domain": "Arctic",
        "resolution": "T127_CORE2"
    }
)

# Intelligent file discovery
restart_files = tellus.find_files(
    simulation="CMIP6_historical_r1i1p1f1",
    content_type=FileContentType.RESTART,
    domain="Arctic", 
    years=(2000, 2014)
)
```

## 🏗️ Architecture

Tellus follows **Domain-Driven Design** principles with clean separation of concerns:

```
src/tellus/
├── domain/          # Pure business logic
│   ├── entities/    # Core domain objects (Simulation, Location, File)
│   ├── repositories/# Data access interfaces  
│   └── services/    # Domain services
├── infrastructure/  # Implementation details (storage protocols, file systems)
├── application/     # Use cases and application services
└── interfaces/      # External interfaces (CLI, API, Web UI)
```

### Core Entities

- **`SimulationEntity`** - Climate model runs with metadata, provenance, and file management
- **`LocationEntity`** - Multi-protocol storage abstraction (SSH, SFTP, tape, cloud)
- **`SimulationFile`** - Semantic file classification with content types and importance levels

## 🎯 Current Features

### Multi-Location Data Management
- **Protocol Support**: SSH, SFTP, local files, tape archives, cloud storage
- **Path Templates**: Dynamic path generation with attribute substitution
- **Smart Discovery**: Automatic file indexing across storage locations

### Semantic File Classification
- **Content Types**: INPUT, OUTPUT, CONFIG, RESTART, LOG, ANALYSIS, VIZ, FORCING
- **Importance Levels**: CRITICAL, IMPORTANT, OPTIONAL, TEMPORARY
- **Archive Handling**: Compressed archives, split files, extraction workflows

### Climate Model Integration
- **Model Support**: FESOM, ECHAM, AWI-CM, and extensible to others
- **Provenance Tracking**: Git commits, build configurations, parameter sets
- **Experiment Organization**: CMIP6 conventions, ensemble management

## 🤖 TerraAI Integration (Roadmap)

Tellus provides the semantic foundation for TerraAI - an AI assistant that understands climate science:

### Intelligent Data Discovery
```
User: "Find all Arctic sea ice experiments from 2023"
TerraAI: *queries Tellus semantic index*
→ Returns simulations with Arctic domain + sea ice variables + 2023 timeframe
```

### Expert System Debugging  
```
User: "Why did my FESOM run crash after 100 days?"
TerraAI: *analyzes logs + code provenance*
→ "Mass conservation violation detected. Try GMRES solver instead of BiCGStab."
```

### Collaboration Networks
```
User: "I need help with ECHAM radiation scheme"
TerraAI: *maps expertise from Git history + calendar integration*
→ "Dr. Mueller has 47 commits in radiation code, available until 3pm today"
```

## 🌌 Planetary Vision

The same abstractions that organize Earth climate simulations can extend to universal planetary modeling:

- **Mars**: Atmospheric dynamics, dust storms, ice cap evolution
- **Venus**: Runaway greenhouse effects, atmospheric composition  
- **Titan**: Methane cycles, hydrocarbon lakes
- **Exoplanets**: Habitability assessment, atmospheric characterization

### AI-Native Modeling Future
Replace traditional numerical solvers with AI models trained on multi-planetary data:

```python
# Traditional: 6 hours for 10-year simulation
# AI-Native: 6 minutes for 100-year simulation

wind_field = ai_atmosphere_model(
    pressure_field=current_pressure,
    planet_params=config,
    historical_context=memory
)
```

## 🚀 Getting Started

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/tellus.git
cd tellus

# Install with pixi (recommended)
pixi install

# Or with pip
pip install -e .
```

### Configuration

1. **Set up storage locations** in `~/.tellus/config.yaml`
2. **Configure model paths** and build environments  
3. **Initialize file discovery** with `tellus scan`

### Demo with AWI Core Repository

Tellus includes integration with AWI's Core Repository for immediate demonstration:

```python
# Load AWI's standard simulations
core_sims = tellus.load_awi_core_repository()

# Explore existing data
for sim in core_sims:
    print(f"{sim.simulation_id}: {sim.get_file_count()} files")
    print(f"  Locations: {sim.get_associated_locations()}")
    print(f"  Model: {sim.model_id}")
```

## 📊 Use Cases

### Research Data Management
- **Problem**: "Where are my FESOM restart files from the Arctic run?"
- **Solution**: `tellus find --type restart --domain Arctic --model FESOM`

### Reproducibility & Provenance  
- **Problem**: "Which code version produced these results?"
- **Solution**: Full Git integration with commit tracking and diff analysis

### Collaboration & Knowledge Transfer
- **Problem**: "Who understands the sea ice dynamics code?"  
- **Solution**: Expertise mapping from Git history and contribution patterns

### Performance Optimization
- **Problem**: "Why is my run slower than expected?"
- **Solution**: Code change correlation with performance metrics

## 🛠️ Development

### Prerequisites
- Python 3.8+
- pixi or conda for environment management
- Git for version control
- SSH access to compute resources (optional)

### Running Tests

```bash
# Unit tests
pixi run pytest tests/

# Integration tests  
pixi run pytest tests/integration/

# Full test suite with coverage
pixi run pytest --cov=tellus tests/
```

### Architecture Decisions

See [ADR/](docs/architecture-decisions/) for detailed architectural decision records covering:
- Domain modeling choices
- Storage abstraction design
- File classification strategies
- AI integration patterns

## 📖 Documentation

- **[User Guide](docs/user-guide.md)** - Getting started with Tellus
- **[API Reference](docs/api/)** - Complete API documentation  
- **[Architecture Overview](docs/architecture.md)** - System design and patterns
- **[Contributing Guide](CONTRIBUTING.md)** - How to contribute to Tellus
- **[Roadmap](ROADMAP.md)** - Future development plans

## 🤝 Contributing

We welcome contributions from the climate modeling community!

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## 🎓 Research & Publications

If you use Tellus in your research, please cite:

```bibtex
@software{gierz2025tellus,
  title={Tellus: Semantic Climate Model Data Management},
  author={Gierz, Paul},
  year={2025},
  institution={Alfred Wegener Institute},
  url={https://github.com/pgierz/tellus}
}
```

## 🏆 Acknowledgments

- **Alfred Wegener Institute** for institutional support
- **AWI TerraAI Initiative** for vision and direction  
- **Climate modeling community** for domain expertise
- **Contributors** who make this project possible

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🌟 Roadmap

### Phase 1: Foundation (Months 1-6)
- ✅ Core domain entities and architecture
- ✅ Multi-location storage abstraction  
- ✅ File discovery and classification
- 🚧 AWI Core Repository integration
- 🚧 CLI interface and basic API

### Phase 2: Intelligence (Months 6-12)  
- 🔄 Semantic understanding layer
- 🔄 Git integration and provenance tracking
- 🔄 TerraAI collaboration features
- 🔄 Expertise mapping and institutional memory

### Phase 3: Scale (Year 2+)
- 📋 Multi-planetary data support
- 📋 AI-native modeling integration  
- 📋 European climate center federation
- 📋 Real-time collaboration platform

## 💬 Community

- **Discussions**: [GitHub Discussions](https://github.com/pgierz/tellus/discussions)
- **Issues**: [GitHub Issues](https://github.com/pgierz/tellus/issues)  
- **Slack**: [AWI TerraAI Slack](https://awi-terraai.slack.com)
- **Email**: [pgierz@awi.de](mailto:pgierz@awi.de)

---

> *"From data chaos to planetary understanding"*  
> **Tellus** - Semantic Climate Science for the AI Age