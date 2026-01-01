use std::{fs::File};
use std::path::PathBuf;
use std::io::Write;
use std::collections::HashSet;
use tgit_core::SafetensorFile;
use tgit_core::ModelArchiver;
use tgit_core::utils::{get_store_path, LockFile};
use tgit_core::remote::RemoteClient;

use clap::{Parser, Subcommand};


#[derive(Parser)]
#[command(name = "tgit")]
#[command(author = env!("CARGO_PKG_AUTHORS"))] 
#[command(version = env!("CARGO_PKG_VERSION"))] 
#[command(about = "Git for Tensors", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Add {
        path: PathBuf,
    },
    Restore {
        path: PathBuf,
        #[arg(long)]
        layers: Option<String>,
    }, 
    Remote {
        #[command(subcommand)]
        action: RemoteCommand,
    },
    Pull {
        #[arg(default_value = "origin")]
        remote: String,
    },
    Push {
        #[arg(default_value = "origin")]
        remote: String,
    },
    Status {
    },
    // Issue #3: Garbage Collection
    Gc,
}

#[derive(Subcommand)]
enum RemoteCommand {
    Add {
        name: String,
        url: String,
    },
    List,
    Remove {
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Add { path } => {
            // Issue #5: Lock File
            let _lock = LockFile::lock()?;

            let path_str = path.to_str().unwrap();

            print!("Adding file: {} ... ", path_str);

            let file = SafetensorFile::open(path_str)?;
            let manifest = file.process(true)?;
            let manifest_json = serde_json::to_string_pretty(&manifest)?;

            let output_path = path.with_extension("tgit.json");
            let mut output_file = File::create(&output_path)?;

            output_file.write_all(manifest_json.as_bytes())?;

            println!("Done! Manifest saved to {}", output_path.to_str().unwrap());

            let store_loc = get_store_path();
            println!("Blobs stored in {}", store_loc.to_str().unwrap());

        }

        Commands::Restore { path, layers } => {
            let file = File::open(&path).expect("Failed to open manifest file");
            let reader = std::io::BufReader::new(file);
            let manifest: tgit_core::storage::TGitManifest = serde_json::from_reader(reader)
                .expect("Failed to parse manifest JSON");

            let output_path = if let Some(file_name) = path.file_name() {
                let name_str = file_name.to_string_lossy();

                let stem = name_str
                    .replace(".tgit.json", "")
                    .replace(".json", "");
                path.with_file_name(format!("{}.safetensors", stem))
            } else {
                PathBuf::from("restored_model.safetensors")
            };

            println!("Restoring to {:?}...", output_path);
            if let Some(l) = layers {
                println!("Partial restore: filtering layers containing '{}'", l);
            }

            match manifest.restore(&output_path, layers.as_deref()) {
                Ok(_) => println!("Restoration complete!"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }

        Commands::Pull { remote } => {
            let config = tgit_core::storage::TGitConfig::load()?;
            if let Some(url) = config.remotes.get(remote) {
                println!("Pulling from remote '{}' at URL '{}'", remote, url);
                
                let client = RemoteClient::new(url)?;
                let paths = std::fs::read_dir(".")?;

                for entry in paths {
                    let entry = entry?;
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with(".tgit.json") {
                            println!("Processing manifest: {}", name);
                            match client.pull(name).await {
                                Ok(manifest) => {
                                    // Update local manifest file
                                    let json = serde_json::to_string_pretty(&manifest)?;
                                    let mut f = File::create(&path)?;
                                    f.write_all(json.as_bytes())?;
                                    println!("Successfully updated {}", name);
                                }
                                Err(e) => eprintln!("Failed to pull {}: {}", name, e),
                            }
                        }
                    }
                }

            } else {
                println!("Remote '{}' not found", remote);
            }
        }
        Commands::Push { remote } => {
            let config = tgit_core::storage::TGitConfig::load()?;
            if let Some(url) = config.remotes.get(remote) {
                println!("Pushing to remote '{}' at URL '{}'", remote, url);
                
                let client = RemoteClient::new(url)?;
                let paths = std::fs::read_dir(".")?;

                for entry in paths {
                    let entry = entry?;
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with(".tgit.json") {
                            println!("Pushing manifest: {}", name);
                            
                            // Load manifest
                            let f = File::open(&path)?;
                            let reader = std::io::BufReader::new(f);
                            let manifest: tgit_core::storage::TGitManifest = serde_json::from_reader(reader)?;

                            match client.push(&manifest, name).await {
                                Ok(_) => println!("Successfully pushed {}", name),
                                Err(e) => eprintln!("Failed to push {}: {}", name, e),
                            }
                        }
                    }
                }

            } else {
                println!("Remote '{}' not found", remote);
            }
        }
        Commands::Status {} => {
            let config = tgit_core::storage::TGitConfig::load()?;
            println!("TGit Configuration Status:");
            println!("Remotes:");
            for (name, url) in &config.remotes {
                println!("  {} -> {}", name, url);
            }
        }

        Commands::Gc => {
            println!("Running Garbage Collection on {}...", get_store_path().display());
            let store_path = get_store_path();
            if !store_path.exists() {
                println!("Store path does not exist.");
                return Ok(());
            }

            // 1. Collect all referenced hashes
            let mut referenced_hashes = HashSet::new();
            let paths = std::fs::read_dir(".")?;
            for entry in paths {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".tgit.json") {
                        let f = File::open(&path)?;
                        let reader = std::io::BufReader::new(f);
                        if let Ok(manifest) = serde_json::from_reader::<_, tgit_core::storage::TGitManifest>(reader) {
                            for tensor in manifest.tensors.values() {
                                referenced_hashes.insert(tensor.hash.clone());
                            }
                        }
                    }
                }
            }
            println!("Found {} referenced blobs in current directory.", referenced_hashes.len());

            // 2. Scan blobs and delete unreferenced
            let mut deleted_count = 0;
            let mut kept_count = 0;
            let blob_paths = std::fs::read_dir(&store_path)?;
            
            for entry in blob_paths {
                let entry = entry?;
                let path = entry.path();
                if let Some(hash) = path.file_name().and_then(|n| n.to_str()) {
                    if !referenced_hashes.contains(hash) {
                        // Delete
                        if let Err(e) = std::fs::remove_file(&path) {
                            eprintln!("Failed to delete blob {}: {}", hash, e);
                        } else {
                            deleted_count += 1;
                        }
                    } else {
                        kept_count += 1;
                    }
                }
            }
            
            println!("GC Complete. Deleted: {}, Kept: {}", deleted_count, kept_count);
            if deleted_count > 0 {
                println!("Warning: Blobs were deleted based only on manifests in the CURRENT directory. If other projects share this store, you may have broken them.");
            }
        }

    // Remote management commands
        Commands::Remote { action } => {
            let mut config = tgit_core::storage::TGitConfig::load()?;

            match action {
                RemoteCommand::Add { name, url } => {
                    config.add_remote(name.clone(), url.clone());
                    config.save()?;
                    println!("Added remote '{}' with URL '{}'", name, url);
                }
                RemoteCommand::List => {
                    println!("Configured remotes:");
                    for (name, url) in &config.remotes {
                        println!("{} -> {}", name, url);
                    }
                }
                RemoteCommand::Remove { name } => {
                    if config.remotes.remove(name).is_some() {
                        config.save()?;
                        println!("Removed remote '{}'", name);
                    } else {
                        println!("Remote '{}' not found", name);
                    }
                }
            }
        }
    }
    Ok(())
}