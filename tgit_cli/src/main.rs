use std::{fs::File};
use std::path::PathBuf;
use std::io::Write;
use tgit_core::SafetensorFile;
use tgit_core::utils::get_store_path;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Add { path } => {
            let path_str = path.to_str().unwrap();

            print!("Adding file: {} ... ", path_str);

            let file = SafetensorFile::open(path_str)?;
            let manifest = file.process(true);
            let manifest_json = serde_json::to_string_pretty(&manifest)?;

            let output_path = path.with_extension("tgit.json");
            let mut output_file = File::create(&output_path)?;

            output_file.write_all(manifest_json.as_bytes())?;

            println!("Done! Manifest saved to {}", output_path.to_str().unwrap());

            let store_loc = get_store_path();
            println!("Blobs stored in {}", store_loc.to_str().unwrap());

        }

        Commands::Restore { path } => {
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

            match manifest.restore(&output_path) {
                Ok(_) => println!("Restoration complete!"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }

        Commands::Pull { remote } => {
            let mut config = tgit_core::storage::TGitConfig::load()?;
            if let Some(url) = config.remotes.get(remote) {
                println!("Pulling from remote '{}' at URL '{}'", remote, url);
                // TODO: Implement pull logic
            } else {
                println!("Remote '{}' not found", remote);
            }
        }
        Commands::Push { remote } => {
            let mut config = tgit_core::storage::TGitConfig::load()?;
            if let Some(url) = config.remotes.get(remote) {
                println!("Pushing to remote '{}' at URL '{}'", remote, url);
                // TODO: Implement push logic
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
            // Additional status information can be added here
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