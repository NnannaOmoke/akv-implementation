use actionkv_db::ActionKV;
use actionkv_db::{ByteStr, ByteString};
use std::collections::HashMap;

#[cfg(target_os = "windows")]
const TEST_USAGE: &str = "
Usage:
    akv_mem.exe FILE get KEY
    akv_mem.exe FILE delete KEY
    akv_mem.exe FILE insert KEY VALUE
    akv_mem.exe FILE update KEY VALUE
";
#[cfg(not(target_os = "windows"))]
const TEST_USAGE: &str = "
Usage:
    akv_mem FILE get KEY
    akv_mem FILE delete KEY
    akv_mem FILE insert KEY VALUE
    akv_mem FILE update KEY VALUE
";

fn store_indx_on_disk(instance: &mut ActionKV, index_key: &ByteStr) -> (){
    instance.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&instance.index).unwrap();
    instance.index = HashMap::new();
    instance.insert(index_key, &index_as_bytes).unwrap();

}

fn main() -> (){
    const INDX_KEY: &ByteStr = b"+index";

    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&TEST_USAGE);
    let action:&str = args.get(2).expect(&TEST_USAGE).as_ref();
    let key: &str = args.get(3).expect(&TEST_USAGE).as_ref();
    let maybe_val = args.get(4);


    let path = std::path::Path::new(fname);
    let mut db = ActionKV::open(path).expect("File could not be loaded!");

    db.load().expect("Data could not be Loaded");

    match action{
        "get" => {
            let index_as_bytes = db.get(&INDX_KEY).unwrap().unwrap();
            let index_decoded = bincode::deserialize(&index_as_bytes);
            let index: HashMap<ByteString, u64> = index_decoded.unwrap();

            match index.get(key.as_bytes()){
                None => eprintln!("{:?} not found", key),
                Some(&var) => {
                    let kv = db.get_at(var).unwrap();
                    println!("{:?}", kv.value)
                }
            }
        }

        "delete" => db.delete(key.as_bytes()).unwrap(),
        "insert" => {
            let val = maybe_val.expect(&TEST_USAGE).as_ref();
            db.update(key.as_bytes(), val).unwrap();
            store_indx_on_disk(&mut db, INDX_KEY);
        },

        "update" => {
            let val = maybe_val.expect(&TEST_USAGE).as_ref();
            db.update(key.as_bytes(), val).unwrap();
            store_indx_on_disk(&mut db, INDX_KEY);
        },

        _ => eprintln!("{}", &TEST_USAGE)
    }

}
