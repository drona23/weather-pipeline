#!/usr/bin/env python3
"""
Setup script for the weather data pipeline.

This script automates the setup process for the weather pipeline project.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stdout:
            print(f"STDOUT: {e.stdout}")
        if e.stderr:
            print(f"STDERR: {e.stderr}")
        return False


def check_python_version():
    """Check if Python version is compatible."""
    print("🐍 Checking Python version...")
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python 3.8+ required, found {version.major}.{version.minor}")
        return False
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True


def create_virtual_environment():
    """Create a virtual environment."""
    venv_path = Path("venv")
    if venv_path.exists():
        print("✅ Virtual environment already exists")
        return True
    
    return run_command("python -m venv venv", "Creating virtual environment")


def install_dependencies():
    """Install Python dependencies."""
    # Determine the pip command based on OS
    if os.name == 'nt':  # Windows
        pip_cmd = "venv\\Scripts\\pip"
    else:  # Unix/Linux/macOS
        pip_cmd = "venv/bin/pip"
    
    return run_command(f"{pip_cmd} install -r requirements.txt", "Installing dependencies")


def create_env_file():
    """Create .env file from template."""
    env_file = Path(".env")
    env_example = Path("env.example")
    
    if env_file.exists():
        print("✅ .env file already exists")
        return True
    
    if env_example.exists():
        shutil.copy(env_example, env_file)
        print("✅ Created .env file from template")
        print("⚠️ Please edit .env file and add your OpenWeatherMap API key")
        return True
    else:
        print("❌ env.example file not found")
        return False


def create_directories():
    """Create necessary directories."""
    directories = ["data", "logs", "dags", "plugins"]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    
    print("✅ Created necessary directories")
    return True


def check_docker():
    """Check if Docker is available."""
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Docker is available")
            return True
        else:
            print("❌ Docker is not available")
            return False
    except FileNotFoundError:
        print("❌ Docker is not installed")
        return False


def start_postgres():
    """Start PostgreSQL using Docker."""
    if not check_docker():
        print("⚠️ Docker not available, skipping PostgreSQL setup")
        print("Please install Docker or set up PostgreSQL manually")
        return False
    
    print("🐘 Starting PostgreSQL with Docker...")
    return run_command("docker-compose up -d postgres", "Starting PostgreSQL")


def print_next_steps():
    """Print next steps for the user."""
    print("\n" + "=" * 60)
    print("🎉 Setup completed successfully!")
    print("=" * 60)
    print("\n📋 Next Steps:")
    print("1. Edit .env file and add your OpenWeatherMap API key")
    print("   - Get a free API key from: https://openweathermap.org/api")
    print("   - Add it to the OPENWEATHER_API_KEY field in .env")
    print("\n2. Activate the virtual environment:")
    if os.name == 'nt':  # Windows
        print("   venv\\Scripts\\activate")
    else:  # Unix/Linux/macOS
        print("   source venv/bin/activate")
    print("\n3. Test the pipeline:")
    print("   python test_pipeline.py")
    print("\n4. Run the pipeline:")
    print("   python src/main.py")
    print("\n5. (Optional) Start Airflow for scheduling:")
    print("   docker-compose --profile airflow up -d")
    print("\n📚 Documentation:")
    print("- Check README.md for detailed instructions")
    print("- Review the code in src/ directory")
    print("- Explore the data/ directory for CSV backups")
    print("\n🔧 Troubleshooting:")
    print("- If tests fail, check the error messages above")
    print("- Ensure PostgreSQL is running: docker-compose ps")
    print("- Check logs: docker-compose logs postgres")
    print("\nHappy Data Engineering! 🚀")


def main():
    """Main setup function."""
    print("🚀 Weather Data Pipeline Setup")
    print("=" * 40)
    
    # Check Python version
    if not check_python_version():
        return False
    
    # Create directories
    if not create_directories():
        return False
    
    # Create virtual environment
    if not create_virtual_environment():
        return False
    
    # Install dependencies
    if not install_dependencies():
        return False
    
    # Create .env file
    if not create_env_file():
        return False
    
    # Start PostgreSQL
    start_postgres()
    
    # Print next steps
    print_next_steps()
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 