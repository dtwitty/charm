use rand::{Error, RngCore};
use wyrand::WyRand;

#[derive(Debug, Clone)]
pub struct CharmRng(WyRand);

impl RngCore for CharmRng {
    fn next_u32(&mut self) -> u32 {
        self.0.next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.fill_bytes(dest)
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Error> {
        self.0.try_fill_bytes(dest)
    }
}

impl CharmRng {
    pub fn new(seed: u64) -> Self {
        Self(WyRand::new(seed))
    }
}