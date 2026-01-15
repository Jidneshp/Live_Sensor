from dataclasses import dataclass

@dataclass
class DataIngestionArtifiact:
    trained_file_path:str
    test_file_path:str