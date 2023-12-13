use std::io::{self, BufReader, Read, Seek, SeekFrom, BufWriter, prelude::*};
use std::{collections::HashMap, path::Path};
use std::fs::{File, OpenOptions};

use byteorder::{ReadBytesExt, LittleEndian, WriteBytesExt};
use crc::crc32;
use serde_derive::{Deserialize, Serialize};


type ByteString = Vec<u8>;
type ByteStr = [u8];


#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair{
    pub key: ByteString,
    pub value: ByteString
}

pub struct ActionKV{
    file: File,
    pub index: HashMap<ByteString, u64>
}

impl ActionKV{
    pub fn open(path: &Path) -> io::Result<ActionKV>{
        let file = OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .create(true)
                                    .append(true)
                                    .open(path)?;
        let index = HashMap::new();
        return Ok(ActionKV{file, index});
    }

    pub fn load(&mut self) -> io::Result<()>{
        let mut file = BufReader::new(&mut self.file);
        loop{
            let pos = file.seek(SeekFrom::Current(0))?;
            let maybe_kv = ActionKV::process_record(&mut file);
            let kv = match maybe_kv{
                Ok(var) => var,
                Err(err) => {
                    match err.kind(){
                        io::ErrorKind::UnexpectedEof => {break;},
                        _ => return Err(err)
                    }
                }
            };
            self.index.insert(kv.key, pos);
        }
        return Ok(());
    }

    pub fn process_record<R: Read>(file: &mut R) -> io::Result<KeyValuePair>{
        let saved_checksum = file.read_u32::<LittleEndian>()?;
        let key_len = file.read_u32::<LittleEndian>()?;
        let val_len= file.read_u32::<LittleEndian>()?;
        let data_len = key_len + val_len;
        let mut data = ByteString::with_capacity(data_len as usize);
        {
            file.by_ref()
                .take(data_len as u64)
                .read_to_end(&mut data)?;
        }

        debug_assert_eq!(data.len(), data_len as usize);
        let checksum = crc32::checksum_ieee(&data);
        if checksum != saved_checksum{
            panic!("Data Corruption Encountered. Wahala Dy. {:08x} != {:08x}", checksum, saved_checksum);
        }
        let value = data.split_off(key_len as usize);
        let key = data;
        return Ok(KeyValuePair{key, value});
    }


    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()>{
        let pos = self.insert_but_ignore_index(key, value)?;
        self.index.insert(key.to_vec(), pos);
        return Ok(());
    }

    pub fn insert_but_ignore_index(
        &mut self,
        key: &ByteStr,
        value: &ByteStr
    ) -> io::Result<u64>{
        let mut file = BufWriter::new(&mut self.file);
        let key_len = key.len();
        let val_len = value.len();

        let mut tmp = ByteString::with_capacity(key_len + val_len);
        for byte in key{
            tmp.push(*byte);
        }
        for byte in value{
            tmp.push(*byte)
        }

        let checksum = crc32::checksum_ieee(&tmp);

        let next_byte = SeekFrom::End(0);
        let current = file.seek(SeekFrom::Current(0))?;
        file.seek(next_byte)?;
        file.write_u32::<LittleEndian>(checksum)?;
        file.write_u32::<LittleEndian>(key_len.try_into().unwrap())?;
        file.write_u32::<LittleEndian>(val_len.try_into().unwrap())?;
        file.write_all(&tmp)?;

        return Ok(current);

    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()>{
        self.insert(key, value)?;
        return Ok(());
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()>{
        self.insert(key, b"")?;
        return Ok(());
    }

    pub fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>>{
        let mut file = BufReader::new(&mut self.file);
        let mut found: Option<(u64, ByteString)> = None;

        loop{
            let pos = file.seek(SeekFrom::Current(0))?;
            let maybe_kv = ActionKV::process_record(&mut file);
            let kv = match maybe_kv{
                Ok(var) => var,
                Err(err) => {
                    match err.kind(){
                        io::ErrorKind::UnexpectedEof =>{break;},
                        _ => return Err(err)
                    }
                }
            };
            if kv.key == target{
                found = Some((pos, kv.value));
            }
        }

        return Ok(found);
    }

    pub fn get_at(&mut self, pos: u64) -> io::Result<KeyValuePair>{
        let mut file = BufReader::new(&mut self.file);
        file.seek(SeekFrom::Start(pos))?;
        let kv = ActionKV::process_record(&mut file)?;
        return Ok(kv);
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>>{
        let pos = match self.index.get(key){
            None => return Ok(None),
            Some(pos) => *pos
        };
        let kv = self.get_at(pos)?;
        return Ok(Some(kv.value));
    }

    pub fn seek_to_end(&mut self) -> io::Result<u64>{
        return self.file.seek(SeekFrom::End(0));
    }
// a lot of extensibility options here
//will come back to that later

}
