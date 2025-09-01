use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

/// Thread-safe buffer pool
pub struct BufferPool {
    /// Pre-allocated buffers
    buffers: Vec<BufferSlot>,

    /// Free buffer indices
    free_list: Arc<Mutex<VecDeque<usize>>>,

    /// Buffer size
    buffer_size: usize,
}

struct BufferSlot {
    data: Vec<u8>,
    in_use: Arc<Mutex<bool>>,
}

impl BufferPool {
    pub fn new(num_buffers: usize, buffer_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(num_buffers);
        let mut free_list = VecDeque::with_capacity(num_buffers);

        for i in 0..num_buffers {
            // Pre-allocate buffer
            let data = vec![0u8; buffer_size];

            buffers.push(BufferSlot {
                data,
                in_use: Arc::new(Mutex::new(false)),
            });

            free_list.push_back(i);
        }

        Self {
            buffers,
            free_list: Arc::new(Mutex::new(free_list)),
            buffer_size,
        }
    }

    /// Get a buffer for reading
    pub fn get_buffer(&self) -> Option<BufferHandle> {
        let mut free_list = self.free_list.lock();

        if let Some(index) = free_list.pop_front() {
            let slot = &self.buffers[index];
            *slot.in_use.lock() = true;

            Some(BufferHandle {
                index,
                data: slot.data.as_ptr(),
                len: self.buffer_size,
                pool: self as *const BufferPool,
            })
        } else {
            None
        }
    }

    /// Return a buffer to the pool
    fn return_buffer(&self, index: usize) {
        if index < self.buffers.len() {
            let slot = &self.buffers[index];
            *slot.in_use.lock() = false;
            self.free_list.lock().push_back(index);
        }
    }

    /// Get a BytesMut buffer for writing
    pub fn get_buffer_mut(&self) -> Option<BufferHandleMut> {
        let mut free_list = self.free_list.lock();

        if let Some(index) = free_list.pop_front() {
            let slot = &self.buffers[index];
            *slot.in_use.lock() = true;

            Some(BufferHandleMut {
                index,
                buffer: BytesMut::from(&slot.data[..]),
                pool: self as *const BufferPool,
            })
        } else {
            None
        }
    }

    /// Create a Bytes view into a buffer
    pub fn buffer_to_bytes(&self, index: usize, len: usize) -> Option<Bytes> {
        if index >= self.buffers.len() || len > self.buffer_size {
            return None;
        }

        let data = &self.buffers[index].data[..len];
        Some(Bytes::copy_from_slice(data))
    }
}

/// Handle to a buffer from the pool
pub struct BufferHandle {
    index: usize,
    data: *const u8,
    len: usize,
    pool: *const BufferPool,
}

impl BufferHandle {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data as *mut u8, self.len) }
    }
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        unsafe {
            let pool = &*self.pool;
            pool.return_buffer(self.index);
        }
    }
}

/// Mutable handle to a buffer
pub struct BufferHandleMut {
    index: usize,
    buffer: BytesMut,
    pool: *const BufferPool,
}

impl BufferHandleMut {
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    pub fn freeze(mut self) -> Bytes {
        self.buffer.split().freeze()
    }
}

impl Drop for BufferHandleMut {
    fn drop(&mut self) {
        unsafe {
            let pool = &*self.pool;
            pool.return_buffer(self.index);
        }
    }
}

// Make handles Send + Sync
unsafe impl Send for BufferHandle {}
unsafe impl Sync for BufferHandle {}
unsafe impl Send for BufferHandleMut {}
unsafe impl Sync for BufferHandleMut {}
