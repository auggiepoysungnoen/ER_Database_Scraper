"""
downloader/__init__.py
======================
Download management package for the Hickey Lab Endometrial Receptivity pipeline.

Exports ``DownloadManager`` and ``FileOrganizer`` so that importers only need
to reference the package root.

Usage
-----
    from downloader import DownloadManager, FileOrganizer

    manager = DownloadManager(
        registry_path="../output/datasets_registry.json",
        output_dir="../output/raw",
    )
    manager.download_all()
    manager.generate_manifest("../output/download_manifest.sh")

    organizer = FileOrganizer(
        raw_dir="../output/raw",
        registry_path="../output/datasets_registry.json",
    )
    organizer.organize()
"""

from downloader.download_manager import DownloadManager
from downloader.file_organizer import FileOrganizer

__all__ = ["DownloadManager", "FileOrganizer"]
