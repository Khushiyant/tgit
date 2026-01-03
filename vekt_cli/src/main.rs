use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use vekt_core::ModelArchiver;
use vekt_core::SafetensorFile;
use vekt_core::remote::RemoteClient;
use vekt_core::utils::{LockFile, find_vekt_root, get_store_path};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "vekt")]
#[command(author = env!("CARGO_PKG_AUTHORS"))]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Git for Tensors", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Add {
        path: PathBuf,
        #[arg(long, default_value = "false")]
        compress: bool,
    },
    Restore {
        path: PathBuf,
        #[arg(long)]
        layers: Option<String>,
    },
    Diff {
        old: PathBuf,
        new: PathBuf,
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
    Status,
    Gc,
}

#[derive(Subcommand)]
enum RemoteCommand {
    Add { name: String, url: String },
    List,
    Remove { name: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Check if repository is initialized for all commands except Init
    if !matches!(cli.command, Commands::Init) && find_vekt_root().is_none() {
        eprintln!("Error: Not a vekt repository (or any parent up to mount point)");
        eprintln!("Run 'vekt init' first to initialize a repository.");
        std::process::exit(1);
    }

    match &cli.command {
        Commands::Init => {
            let current_dir = std::env::current_dir()?;
            let vekt_dir = current_dir.join(".vekt");

            if vekt_dir.exists() {
                println!(
                    "vekt repository already exists in {}",
                    current_dir.display()
                );
                return Ok(());
            }

            std::fs::create_dir_all(&vekt_dir)?;
            std::fs::create_dir_all(vekt_dir.join("blobs"))?;

            // Create default config
            let config = vekt_core::storage::VektConfig::default();
            config.save()?;

            // Create .gitignore to ignore everything in .vekt
            let gitignore_content = "*\n";
            std::fs::write(vekt_dir.join(".gitignore"), gitignore_content)?;
            println!(
                "Initialized empty vekt repository in {}",
                vekt_dir.display()
            );
            println!("\nvekt tracks machine learning models at the tensor level.");
            println!("Use 'vekt add <model.safetensors>' to start tracking a model.");
        }

        Commands::Add { path, compress } => {
            let _lock = LockFile::lock()?;

            let path_str = path.to_str().unwrap();

            print!("Adding file: {} ... ", path_str);

            let file = SafetensorFile::open(path_str)?;
            let manifest = file.process(true)?;
            let manifest_json = serde_json::to_string_pretty(&manifest)?;

            let output_path = path.with_extension("vekt.json");
            let mut output_file = File::create(&output_path)?;

            output_file.write_all(manifest_json.as_bytes())?;

            println!("Done! Manifest saved to {}", output_path.to_str().unwrap());

            let store_loc = get_store_path();
            println!("Blobs stored in {}", store_loc.to_str().unwrap());

            if *compress {
                println!("Note: Compression is enabled but not yet fully integrated. Coming soon!");
            }
        }

        Commands::Diff { old, new } => {
            let old_file = File::open(old)?;
            let new_file = File::open(new)?;

            let old_manifest: vekt_core::storage::VektManifest =
                serde_json::from_reader(std::io::BufReader::new(old_file))?;
            let new_manifest: vekt_core::storage::VektManifest =
                serde_json::from_reader(std::io::BufReader::new(new_file))?;

            old_manifest.print_diff(&new_manifest);
        }

        Commands::Restore { path, layers } => {
            let file = File::open(path).expect("Failed to open manifest file");
            let reader = std::io::BufReader::new(file);
            let manifest: vekt_core::storage::VektManifest =
                serde_json::from_reader(reader).expect("Failed to parse manifest JSON");

            let output_path = if let Some(file_name) = path.file_name() {
                let name_str = file_name.to_string_lossy();

                let stem = name_str.replace(".vekt.json", "").replace(".json", "");
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
            let _lock = LockFile::lock()?;
            let config = vekt_core::storage::VektConfig::load()?;
            if let Some(url) = config.remotes.get(remote) {
                println!("Pulling from remote '{}' at URL '{}'", remote, url);

                let client = RemoteClient::new(url)?;

                // Validate access before attempting operations
                println!("Validating S3 bucket access...");
                if let Err(e) = client.validate_access().await {
                    eprintln!("Failed to validate S3 access: {}", e);
                    return Err(e.into());
                }

                let paths = std::fs::read_dir(".")?;

                for entry in paths {
                    let entry = entry?;
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str())
                        && name.ends_with(".vekt.json")
                    {
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
            } else {
                println!("Remote '{}' not found", remote);
            }
        }
        Commands::Push { remote } => {
            let _lock = LockFile::lock()?;
            let config = vekt_core::storage::VektConfig::load()?;
            if let Some(url) = config.remotes.get(remote) {
                println!("Pushing to remote '{}' at URL '{}'", remote, url);

                let client = RemoteClient::new(url)?;

                // Validate access before attempting operations
                println!("Validating S3 bucket access...");
                if let Err(e) = client.validate_access().await {
                    eprintln!("Failed to validate S3 access: {}", e);
                    return Err(e.into());
                }

                let paths = std::fs::read_dir(".")?;

                for entry in paths {
                    let entry = entry?;
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str())
                        && name.ends_with(".vekt.json")
                    {
                        println!("Pushing manifest: {}", name);

                        // Load manifest
                        let f = File::open(&path)?;
                        let reader = std::io::BufReader::new(f);
                        let manifest: vekt_core::storage::VektManifest =
                            serde_json::from_reader(reader)?;

                        match client.push(&manifest, name).await {
                            Ok(_) => println!("Successfully pushed {}", name),
                            Err(e) => eprintln!("Failed to push {}: {}", name, e),
                        }
                    }
                }
            } else {
                println!("Remote '{}' not found", remote);
            }
        }
        Commands::Status => {
            let config = vekt_core::storage::VektConfig::load()?;
            println!("vekt Configuration Status:");
            println!("Remotes:");
            for (name, url) in &config.remotes {
                println!("  {} -> {}", name, url);
            }
        }

        Commands::Gc => {
            let _lock = LockFile::lock()?;
            println!(
                "Running Garbage Collection on {}...",
                get_store_path().display()
            );

            let root = find_vekt_root().unwrap_or_else(|| PathBuf::from("."));
            match vekt_core::gc::run_gc(&root) {
                Ok(stats) => {
                    println!(
                        "GC Complete. Deleted: {}, Kept: {}",
                        stats.deleted, stats.kept
                    );
                }
                Err(e) => eprintln!("GC Failed: {}", e),
            }
        }

        // Remote management commands
        Commands::Remote { action } => {
            let mut config = vekt_core::storage::VektConfig::load()?;

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
