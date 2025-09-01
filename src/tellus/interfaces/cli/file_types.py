"""
CLI commands for managing file type configurations.

This module provides commands for viewing, editing, and managing the
user-configurable file content type classification system.
"""

import json
from pathlib import Path
from typing import Optional

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from ...domain.entities.file_type_config import (FileTypeConfiguration,
                                                 FileTypeRule,
                                                 get_default_config_path,
                                                 load_file_type_config,
                                                 save_default_config)
from ...domain.entities.simulation_file import FileContentType, FileImportance
from .main import console


@click.group()
def file_types():
    """Manage file content type classification rules"""
    pass


@file_types.command()
@click.option('--config-path', type=click.Path(exists=True, path_type=Path), 
              help='Path to configuration file')
@click.option('--show-patterns', is_flag=True, help='Show detailed pattern information')
def list_rules(config_path: Optional[Path], show_patterns: bool):
    """List all file type classification rules"""
    
    try:
        config = load_file_type_config(config_path)
        
        if not config.rules:
            console.print("[yellow]No file type rules configured[/yellow]")
            return
        
        table = Table(title=f"File Type Classification Rules (v{config.version})")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Content Type", style="green")
        table.add_column("Importance", style="blue")
        table.add_column("Priority", style="magenta")
        
        if show_patterns:
            table.add_column("Patterns", style="yellow")
        
        table.add_column("Description", style="white")
        
        for rule in config.rules:
            row = [
                rule.name,
                rule.content_type.value,
                rule.importance.value,
                str(rule.priority)
            ]
            
            if show_patterns:
                patterns_str = ", ".join(rule.patterns[:3])
                if len(rule.patterns) > 3:
                    patterns_str += f" (+ {len(rule.patterns) - 3} more)"
                row.append(patterns_str)
            
            row.append(rule.description or "")
            table.add_row(*row)
        
        console.print(table)
        
        # Show defaults
        console.print(f"\n[bold]Default Settings:[/bold]")
        console.print(f"  Default Content Type: [green]{config.default_content_type.value}[/green]")
        console.print(f"  Default Importance: [blue]{config.default_importance.value}[/blue]")
        
    except Exception as e:
        console.print(f"[red]Error loading configuration:[/red] {e}")


@file_types.command()
@click.argument('rule_name')
@click.option('--config-path', type=click.Path(path_type=Path), 
              help='Path to configuration file')
def show_rule(rule_name: str, config_path: Optional[Path]):
    """Show detailed information about a specific rule"""
    
    try:
        config = load_file_type_config(config_path)
        rule = config.get_rule(rule_name)
        
        if not rule:
            console.print(f"[red]Rule '{rule_name}' not found[/red]")
            return
        
        # Create detailed display
        details = [
            f"[bold]Name:[/bold] {rule.name}",
            f"[bold]Content Type:[/bold] [green]{rule.content_type.value}[/green]",
            f"[bold]Importance:[/bold] [blue]{rule.importance.value}[/blue]",
            f"[bold]Priority:[/bold] [magenta]{rule.priority}[/magenta]",
            f"[bold]Pattern Type:[/bold] {rule.pattern_type}",
            f"[bold]Case Sensitive:[/bold] {rule.case_sensitive}",
            "",
            f"[bold]Description:[/bold]",
            f"  {rule.description or 'No description'}",
            "",
            f"[bold]Patterns ({len(rule.patterns)}):[/bold]"
        ]
        
        for i, pattern in enumerate(rule.patterns, 1):
            details.append(f"  {i}. {pattern}")
        
        panel = Panel("\n".join(details), title=f"File Type Rule: {rule_name}")
        console.print(panel)
        
    except Exception as e:
        console.print(f"[red]Error loading rule:[/red] {e}")


@file_types.command()
@click.argument('filename')
@click.option('--config-path', type=click.Path(exists=True, path_type=Path), 
              help='Path to configuration file')
@click.option('--show-rules', is_flag=True, help='Show which rule matched')
def test_file(filename: str, config_path: Optional[Path], show_rules: bool):
    """Test file classification for a given filename"""
    
    try:
        config = load_file_type_config(config_path)
        content_type, importance = config.classify_file(filename)
        
        console.print(f"[bold]File:[/bold] {filename}")
        console.print(f"[bold]Content Type:[/bold] [green]{content_type.value}[/green]")
        console.print(f"[bold]Importance:[/bold] [blue]{importance.value}[/blue]")
        
        if show_rules:
            # Find which rule matched
            matched_rule = None
            for rule in config.rules:
                if rule.matches(filename):
                    matched_rule = rule
                    break
            
            if matched_rule:
                console.print(f"[bold]Matched Rule:[/bold] [cyan]{matched_rule.name}[/cyan]")
                console.print(f"[bold]Rule Priority:[/bold] {matched_rule.priority}")
            else:
                console.print("[bold]Matched Rule:[/bold] [yellow]Default (no rule matched)[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Error testing file:[/red] {e}")


@file_types.command()
@click.option('--config-path', type=click.Path(path_type=Path), 
              help='Path to configuration file')
@click.option('--format', 'output_format', type=click.Choice(['json', 'yaml']), 
              default='json', help='Output format')
def export_config(config_path: Optional[Path], output_format: str):
    """Export file type configuration to stdout"""
    
    try:
        config = load_file_type_config(config_path)
        
        if output_format == 'json':
            config_dict = config.to_dict()
            json_output = json.dumps(config_dict, indent=2)
            
            syntax = Syntax(json_output, "json", theme="monokai", line_numbers=True)
            console.print(syntax)
        else:
            console.print("[red]YAML export not yet implemented[/red]")
        
    except Exception as e:
        console.print(f"[red]Error exporting configuration:[/red] {e}")


@file_types.command()
@click.option('--force', is_flag=True, help='Overwrite existing configuration')
@click.option('--config-path', type=click.Path(path_type=Path), 
              help='Path to save configuration file')
def init_config(force: bool, config_path: Optional[Path]):
    """Initialize default file type configuration"""
    
    if config_path is None:
        config_path = get_default_config_path()
    
    if config_path.exists() and not force:
        console.print(f"[yellow]Configuration file already exists: {config_path}[/yellow]")
        console.print("Use --force to overwrite")
        return
    
    try:
        save_default_config(config_path)
        console.print(f"[green]✅ Default configuration saved to:[/green] {config_path}")
        console.print(f"\nEdit this file to customize file type classification rules.")
        console.print(f"Run '[cyan]tellus file-types list-rules[/cyan]' to see the current rules.")
        
    except Exception as e:
        console.print(f"[red]Error creating configuration:[/red] {e}")


@file_types.command()
@click.argument('name')
@click.argument('patterns', nargs=-1, required=True)
@click.option('--content-type', type=click.Choice([ct.value for ct in FileContentType]), 
              required=True, help='File content type')
@click.option('--importance', type=click.Choice([imp.value for imp in FileImportance]), 
              default='important', help='File importance level')
@click.option('--description', help='Description of what this rule matches')
@click.option('--priority', type=int, default=50, help='Rule priority (higher = checked first)')
@click.option('--pattern-type', type=click.Choice(['glob', 'regex']), 
              default='glob', help='Pattern matching type')
@click.option('--case-sensitive', is_flag=True, help='Use case-sensitive matching')
@click.option('--config-path', type=click.Path(path_type=Path), 
              help='Path to configuration file')
def add_rule(name: str, patterns: tuple, content_type: str, importance: str, 
             description: Optional[str], priority: int, pattern_type: str, 
             case_sensitive: bool, config_path: Optional[Path]):
    """Add a new file type classification rule"""
    
    try:
        config = load_file_type_config(config_path)
        
        # Check if rule already exists
        if config.get_rule(name):
            console.print(f"[red]Rule '{name}' already exists[/red]")
            return
        
        # Create new rule
        rule = FileTypeRule(
            name=name,
            patterns=list(patterns),
            content_type=FileContentType(content_type),
            importance=FileImportance(importance),
            description=description or "",
            pattern_type=pattern_type,
            case_sensitive=case_sensitive,
            priority=priority
        )
        
        config.add_rule(rule)
        
        # Save configuration
        if config_path is None:
            config_path = get_default_config_path()
        
        config.save_to_file(config_path)
        
        console.print(f"[green]✅ Rule '{name}' added successfully[/green]")
        console.print(f"Patterns: {', '.join(patterns)}")
        console.print(f"Content Type: [green]{content_type}[/green]")
        console.print(f"Importance: [blue]{importance}[/blue]")
        
    except Exception as e:
        console.print(f"[red]Error adding rule:[/red] {e}")


@file_types.command()
@click.argument('name')
@click.option('--config-path', type=click.Path(path_type=Path), 
              help='Path to configuration file')
def remove_rule(name: str, config_path: Optional[Path]):
    """Remove a file type classification rule"""
    
    try:
        config = load_file_type_config(config_path)
        
        if not config.remove_rule(name):
            console.print(f"[red]Rule '{name}' not found[/red]")
            return
        
        # Save configuration
        if config_path is None:
            config_path = get_default_config_path()
        
        config.save_to_file(config_path)
        
        console.print(f"[green]✅ Rule '{name}' removed successfully[/green]")
        
    except Exception as e:
        console.print(f"[red]Error removing rule:[/red] {e}")


@file_types.command()
@click.option('--config-path', type=click.Path(path_type=Path), 
              help='Path to configuration file')
def validate_config(config_path: Optional[Path]):
    """Validate file type configuration"""
    
    try:
        config = load_file_type_config(config_path)
        
        console.print("[green]✅ Configuration is valid[/green]")
        console.print(f"Rules: {len(config.rules)}")
        console.print(f"Version: {config.version}")
        
        # Test some common filenames
        test_files = [
            "model_output.nc",
            "config.nml", 
            "simulation.log",
            "restart_001.rst",
            "plot.png",
            "run_script.sh"
        ]
        
        console.print("\n[bold]Sample Classifications:[/bold]")
        table = Table()
        table.add_column("Filename", style="cyan")
        table.add_column("Content Type", style="green")
        table.add_column("Importance", style="blue")
        
        for filename in test_files:
            content_type, importance = config.classify_file(filename)
            table.add_row(filename, content_type.value, importance.value)
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Configuration validation failed:[/red] {e}")


if __name__ == "__main__":
    file_types()