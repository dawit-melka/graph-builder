from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Any, Optional, Union
import json
import shutil
import tempfile
import subprocess
import os
import uuid

app = FastAPI(title="HugeGraph Loader API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration for HugeGraph loader
HUGEGRAPH_LOADER_PATH = "/home/developer/Desktop/projects/incubator-hugegraph-toolchain/hugegraph-loader/apache-hugegraph-loader-incubating-1.5.0/bin/hugegraph-loader.sh"
HUGEGRAPH_HOST = "localhost"
HUGEGRAPH_PORT = "8080"
HUGEGRAPH_GRAPH = "hugegraph"

# Schema Models
class PropertyKey(BaseModel):
    name: str
    type: str  # text, int, double, etc.
    # Optional properties
    cardinality: Optional[str] = None  # single, list, set
    # Other optional properties
    options: Optional[Dict[str, Any]] = None

class VertexLabel(BaseModel):
    name: str
    properties: List[str]
    primary_keys: Optional[List[str]] = None
    nullable_keys: Optional[List[str]] = None
    id_strategy: Optional[str] = None  # primary_key, automatic, customize_number, customize_string
    # Other optional properties
    options: Optional[Dict[str, Any]] = None

class EdgeLabel(BaseModel):
    name: str
    source_label: str
    target_label: str
    properties: Optional[List[str]] = None
    sort_keys: Optional[List[str]] = None
    # Other optional properties
    options: Optional[Dict[str, Any]] = None

class SchemaDefinition(BaseModel):
    property_keys: List[PropertyKey]
    vertex_labels: List[VertexLabel]
    edge_labels: List[EdgeLabel]

class HugeGraphLoadResponse(BaseModel):
    job_id: str
    status: str
    message: str
    details: Optional[Dict[str, Any]] = None
    schema_groovy: Optional[str] = None


def json_to_groovy(schema_json: Union[Dict, SchemaDefinition]) -> str:
    """
    Convert JSON schema definition to HugeGraph Groovy schema format.
    
    Args:
        schema_json: The schema definition in JSON format or as a SchemaDefinition object
        
    Returns:
        The equivalent schema in Groovy format
    """
    if isinstance(schema_json, dict):
        schema = SchemaDefinition(**schema_json)
    else:
        schema = schema_json
    
    groovy_lines = []
    
    # Process property keys
    for prop in schema.property_keys:
        line = f'schema.propertyKey("{prop.name}").as{prop.type.capitalize()}()'
        
        if prop.cardinality:
            line += f'.cardinality("{prop.cardinality}")'
        
        # Add any additional options from the options dictionary
        if prop.options:
            for opt_name, opt_value in prop.options.items():
                if isinstance(opt_value, str):
                    line += f'.{opt_name}("{opt_value}")'
                else:
                    line += f'.{opt_name}({opt_value})'
        
        line += '.ifNotExist().create();'
        groovy_lines.append(line)
    
    groovy_lines.append("")  # Empty line for readability
    
    # Process vertex labels
    for vertex in schema.vertex_labels:
        line = f'schema.vertexLabel("{vertex.name}")'
        
        # Add ID strategy if defined
        if vertex.id_strategy:
            if vertex.id_strategy == "primary_key":
                line += '.useCustomizeStringId()'  # This will be overridden by primaryKeys
            elif vertex.id_strategy == "customize_number":
                line += '.useCustomizeNumberId()'
            elif vertex.id_strategy == "customize_string":
                line += '.useCustomizeStringId()'
            elif vertex.id_strategy == "automatic":
                line += '.useAutomaticId()'
        
        # Add properties
        if vertex.properties:
            props_str = ', '.join([f'"{prop}"' for prop in vertex.properties])
            line += f'.properties({props_str})'
        
        # Add primary keys
        if vertex.primary_keys:
            keys_str = ', '.join([f'"{key}"' for key in vertex.primary_keys])
            line += f'.primaryKeys({keys_str})'
        
        # Add nullable keys
        if vertex.nullable_keys:
            keys_str = ', '.join([f'"{key}"' for key in vertex.nullable_keys])
            line += f'.nullableKeys({keys_str})'
        
        # Add any additional options
        if vertex.options:
            for opt_name, opt_value in vertex.options.items():
                if isinstance(opt_value, str):
                    line += f'.{opt_name}("{opt_value}")'
                else:
                    line += f'.{opt_name}({opt_value})'
        
        line += '.ifNotExist().create();'
        groovy_lines.append(line)
    
    groovy_lines.append("")  # Empty line for readability
    
    # Process edge labels
    for edge in schema.edge_labels:
        line = f'schema.edgeLabel("{edge.name}")'
        line += f'.sourceLabel("{edge.source_label}")'
        line += f'.targetLabel("{edge.target_label}")'
        
        # Add properties
        if edge.properties:
            props_str = ', '.join([f'"{prop}"' for prop in edge.properties])
            line += f'.properties({props_str})'
        
        # Add sort keys
        if edge.sort_keys:
            keys_str = ', '.join([f'"{key}"' for key in edge.sort_keys])
            line += f'.sortKeys({keys_str})'
        
        # Add any additional options
        if edge.options:
            for opt_name, opt_value in edge.options.items():
                if isinstance(opt_value, str):
                    line += f'.{opt_name}("{opt_value}")'
                else:
                    line += f'.{opt_name}({opt_value})'
        
        line += '.ifNotExist().create();'
        groovy_lines.append(line)
    
    return '\n'.join(groovy_lines)


@app.post("/api/load", response_model=HugeGraphLoadResponse)
async def load_data(
    files: List[UploadFile] = File(...),
    config: str = Form(...),
    schema_json: Optional[str] = Form(None),
):
    """
    API endpoint to load data into HugeGraph.
    
    - files: Multiple data files to be loaded
    - config: JSON configuration similar to struct.json
    - schema_json: Optional schema in JSON format that will be converted to Groovy
    """
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    try:
        # Parse the config
        config_data = json.loads(config)
        
        # Create a temporary directory for this job
        with tempfile.TemporaryDirectory() as tmpdir:
            # Dictionary to map original filenames to their paths in the temp directory
            file_mapping = {}
            
            # Save all uploaded files to temp directory
            for file in files:
                file_path = os.path.join(tmpdir, file.filename)
                with open(file_path, "wb") as f:
                    shutil.copyfileobj(file.file, f)
                file_mapping[file.filename] = file_path
            
            # Convert JSON schema to Groovy if provided
            schema_path = None
            schema_groovy = None
            if schema_json:
                schema_data = json.loads(schema_json)
                schema_groovy = json_to_groovy(schema_data)
                schema_path = os.path.join(tmpdir, f"schema-{job_id}.groovy")
                with open(schema_path, "w") as f:
                    f.write(schema_groovy)
            
            # Update the paths in the config to point to the temp files
            updated_config = update_file_paths_in_config(config_data, file_mapping)
            
            # Save the updated config to a file
            config_path = os.path.join(tmpdir, f"struct-{job_id}.json")
            with open(config_path, "w") as f:
                json.dump(updated_config, f, indent=2)
            
            # Build the HugeGraph loader command
            cmd = [
                "sh", HUGEGRAPH_LOADER_PATH,
                "-g", HUGEGRAPH_GRAPH,
                "-f", config_path,
                "-h", HUGEGRAPH_HOST,
                "-p", HUGEGRAPH_PORT
            ]
            
            if schema_path:
                cmd.extend(["-s", schema_path])
            
            # Run the command
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                return HugeGraphLoadResponse(
                    job_id=job_id,
                    status="error",
                    message=f"HugeGraph loader failed with exit code {result.returncode}",
                    details={
                        "stdout": result.stdout,
                        "stderr": result.stderr
                    },
                    schema_groovy=schema_groovy
                )
            
            return HugeGraphLoadResponse(
                job_id=job_id,
                status="success",
                message="Data loaded successfully",
                details={
                    "stdout": result.stdout
                },
                schema_groovy=schema_groovy
            )
            
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")


def update_file_paths_in_config(config, file_mapping):
    """
    Update file paths in the config to use the temporary file paths.
    
    Args:
        config: The configuration dictionary
        file_mapping: Dictionary mapping original filenames to temp file paths
    
    Returns:
        Updated configuration dictionary
    """
    # Create a deep copy of the config to avoid modifying the original
    updated_config = json.loads(json.dumps(config))
    
    # Function to process vertices and edges
    def update_paths(items):
        for item in items:
            if "input" in item and item["input"].get("type") == "file":
                # Extract just the filename from the path
                original_path = item["input"]["path"]
                filename = os.path.basename(original_path)
                
                # Check if this filename exists in our mapping
                if filename in file_mapping:
                    item["input"]["path"] = file_mapping[filename]
                # Otherwise, keep the original path (might be a relative path or a URL)
        
        return items
    
    # Update paths in vertices and edges
    if "vertices" in updated_config:
        updated_config["vertices"] = update_paths(updated_config["vertices"])
    
    if "edges" in updated_config:
        updated_config["edges"] = update_paths(updated_config["edges"])
    
    return updated_config


@app.post("/api/convert-schema", response_class=JSONResponse)
async def convert_schema(schema_json: Dict = None):
    """
    Convert JSON schema to Groovy format without loading data.
    
    Args:
        schema_json: The schema definition in JSON format
        
    Returns:
        The equivalent schema in Groovy format
    """
    try:
        groovy_schema = json_to_groovy(schema_json)
        return {
            "status": "success",
            "schema_groovy": groovy_schema
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error converting schema: {str(e)}")


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)