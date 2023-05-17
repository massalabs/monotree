use monotree::{Monotree, Database, Result, hasher::Blake3, Hasher};
use std::collections::{HashMap, BTreeMap};

use base64::engine::general_purpose::GeneralPurpose;
use base64::alphabet::BCRYPT;
use base64::engine::general_purpose::GeneralPurposeConfig;
use base64::engine::Engine;

const B64 : GeneralPurpose = GeneralPurpose::new(&BCRYPT, GeneralPurposeConfig::new());
const EXPECTED_HASH : &str = "c5YPE5bjoc3CRcYVnxFoMsbuFyVCbhBfOiVwPEsGBzO=";

fn b64(bytes: &[u8]) -> String {
    B64.encode(bytes)
}

struct CustomMonotreeDB(HashMap<[u8; 32], Option<Vec<u8>>>);
impl Database for CustomMonotreeDB {
    fn new(_: &str) -> CustomMonotreeDB {
        CustomMonotreeDB(HashMap::new())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut fixed_size_key = [0u8; 32];
        fixed_size_key.copy_from_slice(key);
        if let Some(val) = self.0.get(key) {
            Ok(val.clone())
        } else {
            Ok(None)
        }
    }

    fn put(&mut self, key: &[u8], val: Vec<u8>) -> Result<()> {
        let mut fixed_size_key = [0u8; 32];
        fixed_size_key.copy_from_slice(key);
        self.0.insert(fixed_size_key, Some(val));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let mut fixed_size_key = [0u8; 32];
        fixed_size_key.copy_from_slice(key);
        self.0.insert(fixed_size_key, None);
        Ok(())
    }

    fn init_batch(&mut self) -> Result<()> {
        unimplemented!();
    }

    fn finish_batch(&mut self) -> Result<()> {
        unimplemented!();
    }
}

type DBBatch = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

fn add_to_batch(batch: &mut DBBatch, bytes: &[u8]) {
    let hasher = Blake3::new();
    let hash = hasher.digest(bytes);
    batch.insert(hash.to_vec(), Some(hash.to_vec()));
}

fn apply_batch(tree: &mut Monotree<CustomMonotreeDB, Blake3>, changes: &DBBatch, root: Option<[u8; 32]>) -> [u8; 32] {
    let hasher = Blake3::new();
    let mut new_root = root;
    for (key, val) in changes.iter() {
        let keyh = hasher.digest(key);
        if let Some(v) = val {
            let valh = hasher.digest(v);
            println!("Insert {}: {}", b64(&keyh), b64(&valh));
            new_root = tree.insert(new_root.as_ref(), &keyh, &valh).unwrap()
        } else {
            println!("Remove {}", b64(&keyh));
            new_root = tree.remove(new_root.as_ref(), &keyh).unwrap()
        }
    }
    return new_root.unwrap();
}

fn main() -> Result<()> {
    let mut tree_a: Monotree<CustomMonotreeDB, Blake3> = Monotree::new("tree_a");
    let mut tree_b: Monotree<CustomMonotreeDB, Blake3> = Monotree::new("tree_b");

    let mut change_a = DBBatch::new();
    let mut change_b = DBBatch::new();
    let mut change_c = DBBatch::new();

    for i in 0u8..20 {
        if i < 12 {
            add_to_batch(&mut change_a, &[i]);
        }
        if i > 8 {
            add_to_batch(&mut change_b, &[i]);
        }
        add_to_batch(&mut change_c, &[i]);
    }

    assert!(change_a.iter().all(|(key, _)| change_c.contains_key(key)));
    assert!(change_b.iter().all(|(key, _)| change_c.contains_key(key)));


    println!("\nApply A");
    let root_a = apply_batch(&mut tree_a, &change_a, None);
    println!("\nApply B");
    let root_a = apply_batch(&mut tree_a, &change_b, Some(root_a));
    println!("\nApply C");
    let root_b = apply_batch(&mut tree_b, &change_c, None);

    assert_eq!(b64(&root_b).as_str(), EXPECTED_HASH);
    assert_eq!(root_a, root_b);

    Ok(())
}