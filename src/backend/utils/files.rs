use std::fs;
use std::path::Path;
use std::io;

pub fn clear_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    if path.as_ref().is_dir() {
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                fs::remove_dir_all(&entry_path)?;
            } else {
                fs::remove_file(&entry_path)?;
            }
        }
    }
    println!("directory cleared.");
    Ok(())
}
