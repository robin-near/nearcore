pub trait UnsafeSerde: Copy {
    fn unsafe_serialize(&self, target: &mut [u8]) {
        unsafe {
            let target: *mut u8 = target.as_mut_ptr();
            let target_transmuted: *mut Self = std::mem::transmute(target);
            *target_transmuted = *self;
        }
    }
    fn unsafe_deserialize(data: &[u8]) -> Self {
        unsafe {
            let data: *const u8 = data.as_ptr();
            let data_transmuted: *const Self = std::mem::transmute(data);
            *data_transmuted
        }
    }
}
