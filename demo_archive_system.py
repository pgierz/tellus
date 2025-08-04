#!/usr/bin/env python3
"""Demo workflows for the new archive system"""

import json
import sys
import tarfile
import tempfile
from pathlib import Path

# Add the tellus package to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tellus.simulation.simulation import (ArchiveManifest, ArchiveRegistry,
                                          CacheConfig, CacheManager,
                                          CLIProgressCallback,
                                          CompressedArchive, PathMapper,
                                          PathMapping, TagSystem)


def create_demo_archive(archive_path: Path, simulation_data: dict) -> Path:
    """Create a demo tar.gz archive with sample simulation files"""
    print(f"Creating demo archive: {archive_path}")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create sample directory structure
        (temp_path / "input").mkdir()
        (temp_path / "scripts").mkdir()
        (temp_path / "output").mkdir()
        (temp_path / "namelists").mkdir()

        # Create sample files
        files_to_create = {
            "input/forcing_data.nc": b"Sample forcing data content",
            "input/initial_conditions.nc": b"Sample initial conditions",
            "scripts/run_model.sh": b"#!/bin/bash\necho 'Running model'\n",
            "scripts/postprocess.py": b"print('Post-processing data')\n",
            "output/model_output_2023.nc": b"Sample model output 2023",
            "output/diagnostics.nc": b"Sample diagnostics data",
            "namelists/model.nml": b"&model_params\n  timestep = 3600\n/\n",
            "README.txt": b"This is a demo simulation archive",
        }

        for file_path, content in files_to_create.items():
            full_path = temp_path / file_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_bytes(content)

        # Create metadata file
        metadata_file = temp_path / "simulation_metadata.json"
        metadata_file.write_text(json.dumps(simulation_data, indent=2))

        # Create the archive
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive_path, "w:gz") as tar:
            # Add files individually to avoid ./ prefix
            for file_path, content in files_to_create.items():
                full_path = temp_path / file_path
                tar.add(full_path, arcname=file_path)
            # Add metadata file
            tar.add(metadata_file, arcname="simulation_metadata.json")

    print(f"✓ Created archive with {len(files_to_create) + 1} files")
    return archive_path


def demo_basic_archive_operations():
    """Demo 1: Basic archive operations"""
    print("\n" + "=" * 60)
    print("DEMO 1: Basic Archive Operations")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create demo archive
        archive_path = temp_path / "demo_simulation.tar.gz"
        create_demo_archive(
            archive_path,
            {
                "simulation_id": "demo_001",
                "model": "ECMWF-IFS",
                "created": "2024-01-15",
            },
        )

        # Initialize archive system
        print("\n1. Setting up archive system...")
        cache_config = CacheConfig(
            cache_dir=temp_path / "cache",
            archive_cache_size_limit=100 * 1024**2,  # 100MB
            file_cache_size_limit=50 * 1024**2,  # 50MB
        )
        cache_manager = CacheManager(cache_config)

        # Test location loading (optional - will work without)
        location = None
        try:
            from tellus.location import Location

            # Try to load existing locations
            Location.load_locations()
            # For the demo, only use local locations to avoid remote file issues
            locations = [
                loc
                for loc in Location.list_locations()
                if any(kind.name in ["DISK", "COMPUTE"] for kind in loc.kinds)
            ]
            if locations:
                location = locations[0]
                print(
                    f"   Using location: {location.name} ({', '.join(k.name for k in location.kinds)})"
                )
            else:
                print("   No local locations configured, using local filesystem")
        except Exception as e:
            print(f"   Location loading failed: {e}, using local filesystem")

        # Create archive instance
        archive = CompressedArchive(
            archive_id="demo_001",
            archive_location=str(archive_path),
            cache_manager=cache_manager,
            location=location,
        )

        # Add progress callback
        progress = CLIProgressCallback(verbose=True)
        archive.add_progress_callback(progress)

        print("\n2. Archive status:")
        status = archive.status()
        for key, value in status.items():
            print(f"   {key}: {value}")

        print("\n3. Listing all files:")
        files = archive.list_files()
        for file_path, tagged_file in files.items():
            tags_str = ", ".join(tagged_file.tags)
            print(f"   {file_path} [{tags_str}] ({tagged_file.size} bytes)")

        print("\n4. Getting files by tags:")
        input_files = archive.get_files_by_tags("input")
        print(f"   Found {len(input_files)} input files:")
        for file_path in input_files:
            print(f"   - {file_path}")

        script_files = archive.get_files_by_tags("scripts")
        print(f"   Found {len(script_files)} script files:")
        for file_path in script_files:
            print(f"   - {file_path}")

        print("\n5. Extracting a single file:")
        extract_dir = temp_path / "extracted"
        extracted_path = archive.extract_file("scripts/run_model.sh", extract_dir)
        print(f"   Extracted to: {extracted_path}")
        print(f"   Content: {extracted_path.read_text()}")

        print("\n6. Cache statistics:")
        cache_stats = cache_manager.get_cache_stats()
        for key, value in cache_stats.items():
            if isinstance(value, int) and value > 1024:
                value = f"{value / 1024:.1f} KB"
            print(f"   {key}: {value}")


def demo_multi_archive_registry():
    """Demo 2: Multi-archive registry with smart file resolution"""
    print("\n" + "=" * 60)
    print("DEMO 2: Multi-Archive Registry")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create multiple demo archives
        archives_data = [
            {
                "name": "input_archive",
                "simulation_id": "demo_002a",
                "model": "ECMWF-IFS",
                "type": "input_data",
            },
            {
                "name": "output_archive",
                "simulation_id": "demo_002b",
                "model": "ECMWF-IFS",
                "type": "model_output",
            },
        ]

        print("\n1. Creating multiple archives...")
        archive_paths = []
        for i, data in enumerate(archives_data):
            archive_path = temp_path / f"{data['name']}.tar.gz"
            create_demo_archive(archive_path, data)
            archive_paths.append(archive_path)

        # Setup archive registry
        print("\n2. Setting up archive registry...")
        registry = ArchiveRegistry(
            simulation_id="demo_002",
            cache_manager=CacheManager(CacheConfig(cache_dir=temp_path / "cache")),
        )

        # Add archives to registry
        for i, archive_path in enumerate(archive_paths):
            data = archives_data[i]
            archive = CompressedArchive(
                archive_id=data["simulation_id"], archive_location=str(archive_path)
            )
            archive.add_progress_callback(CLIProgressCallback(verbose=True))
            registry.add_archive(archive, data["name"])

        print(f"\n3. Registry contains {len(registry.list_archives())} archives:")
        for name in registry.list_archives():
            print(f"   - {name}")

        print("\n4. Finding files across archives:")
        # This file should exist in both archives
        matches = registry.find_file("README.txt")
        print(f"   Found 'README.txt' in {len(matches)} archives:")
        for match in matches:
            print(
                f"   - Archive: {match['archive_name']} (size: {match['size']} bytes)"
            )

        print("\n5. Smart file extraction (chooses best archive):")
        extract_dir = temp_path / "smart_extracted"
        try:
            extracted_path = registry.extract_file_smart("README.txt", extract_dir)
            print(f"   Extracted to: {extracted_path}")
            print(f"   Content: {extracted_path.read_text()}")
        except Exception as e:
            print(f"   Error: {e}")

        print("\n6. Extract all input files from all archives:")
        results = registry.extract_files_by_tags(extract_dir, "input")
        for archive_name, extracted_files in results.items():
            print(f"   From {archive_name}: {len(extracted_files)} files")
            for file_path in extracted_files:
                print(f"     - {file_path}")

        print("\n7. Combined statistics:")
        stats = registry.get_combined_stats()
        for key, value in stats.items():
            if key == "combined_tags":
                print(f"   {key}:")
                for tag, count in value.items():
                    print(f"     {tag}: {count} files")
            else:
                print(f"   {key}: {value}")


def demo_path_mapping():
    """Demo 3: Path mapping and customization"""
    print("\n" + "=" * 60)
    print("DEMO 3: Path Mapping and Customization")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create demo archive
        archive_path = temp_path / "demo_with_mapping.tar.gz"
        create_demo_archive(
            archive_path, {"simulation_id": "demo_003", "model": "ECMWF-IFS"}
        )

        print("\n1. Setting up path mapping...")
        # Create custom path mapping
        path_mapping = PathMapping(
            strip_prefixes=["old_sim_name/", "archive_prefix/"],
            add_prefix="simulations/{{simulation_id}}/",
            relocations={
                "input/*": "data/input/*",
                "output/*": "data/output/*",
                "scripts/*": "tools/scripts/*",
            },
            template_variables={"simulation_id": "demo_003", "model_id": "IFS"},
        )

        path_mapper = PathMapper()
        path_mapper.set_archive_mapping("demo_003", path_mapping)

        # Create archive with path mapping
        archive = CompressedArchive(
            archive_id="demo_003",
            archive_location=str(archive_path),
            path_mapper=path_mapper,
        )

        archive.add_progress_callback(CLIProgressCallback(verbose=True))

        print("\n2. Testing path mappings:")
        test_paths = [
            "input/forcing_data.nc",
            "output/model_output_2023.nc",
            "scripts/run_model.sh",
            "namelists/model.nml",
        ]

        for original_path in test_paths:
            mapped_path = path_mapper.map_path(original_path, "demo_003")
            print(f"   {original_path} → {mapped_path}")

        print("\n3. Extracting with path mapping:")
        extract_dir = temp_path / "mapped_extraction"

        # Extract files with mapping applied
        for test_path in test_paths[:2]:  # Just test first 2 files
            try:
                extracted_path = archive.extract_file(
                    test_path, extract_dir, apply_path_mapping=True
                )
                print(f"   {test_path} → {extracted_path}")
            except Exception as e:
                print(f"   Error extracting {test_path}: {e}")

        print("\n4. Directory structure after extraction:")
        if extract_dir.exists():
            for path in sorted(extract_dir.rglob("*")):
                if path.is_file():
                    rel_path = path.relative_to(extract_dir)
                    print(f"   {rel_path}")


def demo_custom_tags():
    """Demo 4: Custom tagging system"""
    print("\n" + "=" * 60)
    print("DEMO 4: Custom Tagging System")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create demo archive
        archive_path = temp_path / "demo_custom_tags.tar.gz"
        create_demo_archive(
            archive_path, {"simulation_id": "demo_004", "model": "Custom-Model"}
        )

        print("\n1. Setting up custom tag system...")
        # Create custom tag patterns
        custom_tag_patterns = {
            "forcing": ["input/forcing*", "forcing/*"],
            "initial": ["input/initial*", "ic/*"],
            "postprocess": ["scripts/*post*", "scripts/*process*"],
            "diagnostics": ["*diagnostic*", "*diag*"],
            "netcdf": ["*.nc"],
            "executables": ["*.sh", "*.exe"],
            "config": ["*.nml", "*.cfg", "*.yaml", "*.json"],
        }

        tag_system = TagSystem(custom_tag_patterns)

        # Test tagging on sample files
        test_files = [
            "input/forcing_data.nc",
            "input/initial_conditions.nc",
            "scripts/postprocess.py",
            "output/diagnostics.nc",
            "scripts/run_model.sh",
            "namelists/model.nml",
        ]

        print("\n2. Testing custom tags:")
        for file_path in test_files:
            tags = tag_system.tag_file(file_path)
            tags_str = ", ".join(sorted(tags))
            print(f"   {file_path} → [{tags_str}]")

        print("\n3. Discovering potential tags from archive:")
        archive = CompressedArchive(
            archive_id="demo_004", archive_location=str(archive_path)
        )

        # Ensure manifest is loaded first
        archive.refresh_manifest()

        # Override the tag system
        archive.manifest.tag_system = tag_system
        tag_system._compile_patterns()  # Recompile with new patterns

        # Re-tag all files
        for file_path in archive.manifest.files:
            new_tags = tag_system.tag_file(file_path)
            archive.manifest.files[file_path].tags = new_tags

        print("\n4. Files by custom tags:")
        tag_counts = archive.list_tags()
        for tag, count in sorted(tag_counts.items()):
            print(f"   {tag}: {count} files")

        print("\n5. Extract all NetCDF files:")
        extract_dir = temp_path / "netcdf_files"
        netcdf_files = archive.get_files_by_tags("netcdf")

        print(f"   Found {len(netcdf_files)} NetCDF files:")
        for file_path in netcdf_files:
            print(f"   - {file_path}")


def main():
    """Run all demo workflows"""
    print("Archive System Demo Workflows")
    print("=" * 60)

    try:
        demo_basic_archive_operations()
        demo_multi_archive_registry()
        demo_path_mapping()
        demo_custom_tags()

        print("\n" + "=" * 60)
        print("✓ All demos completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Demo failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
