use std::cmp::Ordering;


/// A buffer of inflight messages
#[derive(Debug, PartialEq, Clone)]
pub struct Inflights{
    /// the starting index in the buffer
    start: usize,
    /// number of inflight in the buffer
    count: usize,
    /// ring buffer
    buffer: Vec<u64>,
    /// capacity
    cap: usize,
    /// To support dynamically change inflight size.
    incoming_cap: Option<usize>,
}

impl Inflights{
    /// Creates a new buffer for inflight messages.
    pub fn new(cap: usize) -> Inflights{
        Inflights { start:0, count: 0, buffer: Vec::with_capacity(cap), cap, incoming_cap: None }
    }

    /// Adjust inflight buffer capacity. Set it to `0` will disable the progress. Calling it between `self.full()` and `self.add()` can cause a panic.
    pub fn set_cap(&mut self, incoming_cap: usize) {
        match self.cap.cmp(&incoming_cap) {
            Ordering::Equal => self.incoming_cap = None,
            Ordering::Less => {
                if self.start + self.count <= self.cap {
                    if self.buffer.capacity() > 0 {
                        self.buffer.reserve(incoming_cap - self.buffer.len());
                    }
                } else {
                    debug_assert_eq!(self.cap, self.buffer.len());
                    let mut buffer = Vec::with_capacity(incoming_cap);
                    buffer.extend_from_slice(&self.buffer[self.start..]);
                    buffer.extend_from_slice(&self.buffer[0..self.count - (self.cap - self.start)]);
                    self.buffer = buffer;
                    self.start = 0;
                }
                self.cap = incoming_cap;
                self.incoming_cap = None;
            },
            Ordering::Greater => {
                if self.count == 0 {
                    self.cap = incoming_cap;
                    self.incoming_cap = None;
                    self.start = 0;
                    if self.buffer.capacity() > 0 {
                        self.buffer = Vec::with_capacity(incoming_cap);
                    }
                } else {
                    self.incoming_cap = Some(incoming_cap);
                }
            }
        }
    }

    /// Returns true if inflights is full
    #[inline]
    pub fn full(&self) -> bool{
        self.count == self.cap || self.incoming_cap.map_or(false, |cap| self.count >= cap)
    }

    /// Adds an inflight into inflights
    pub fn add(&mut self, inflights: u64) {
        if self.full() {
            panic!("cannot add into a full inflights")
        }

        if self.buffer.capacity() == 0 {
            debug_assert_eq!(self.count, 0);
            debug_assert_eq!(self.start, 0);
            debug_assert!(self.incoming_cap.is_none());
            self.buffer = Vec::with_capacity(self.cap);
        }   

        let mut next = self.start + self.count;
        if next >= self.cap {
            next -= self.cap;
        }
        assert!(next <= self.buffer.len());
        if next == self.buffer.len() {
            self.buffer.push(inflights);
        }else {
            self.buffer[next] = inflights;
        }
        self.count += 1;
    }

    /// Frees the inflights smaller or equal to the given `to` flight.
    pub fn free_to(&mut self, to: u64) {
        if self.count == 0 || to < self.buffer[self.start] {
            // out of the left side the window
            return;
        }

        let mut i = 0usize;
        let mut idx = self.start;
        while i < self.count {
            if to < self.buffer[idx] {
                // find the first large inflight
                break;
            }

            // incresse index and maybe rotate
            idx += 1;
            if idx >= self.cap {
                idx -= self.cap;
            }

            i += 1;
        }

        // free i inflight and set new start index
        self.count -= i;
        self.start = idx;

        if self.count == 0 {
            if let Some(incoming_cap) = self.incoming_cap.take() {
                self.start = 0;
                self.cap = incoming_cap;
                self.buffer = Vec::with_capacity(self.cap);
            }
        }
    }

    /// Frees the first buffer entry.
    #[inline]
    pub fn free_first_one(&mut self){
        if self.count > 0 {
            let start = self.buffer[self.start];
            self.free_to(start);
        }
    }

    /// Frees all inflights 
    #[inline]
    pub fn reset(&mut self){
        self.count = 0;
        self.start = 0;
        self.buffer = vec![];
        self.cap = self.incoming_cap.take().unwrap_or(self.cap);
    }

    /// Number of inflight messages. It's for tests.
    #[doc(hidden)]
    #[inline]
    pub fn count(&self) -> usize{
        self.count
    }

    /// Capacity of the internal buffer.
    #[doc(hidden)]
    #[inline]
    pub fn buffer_capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Whether buffer is allocated or not. It's for tests.
    #[doc(hidden)]
    #[inline]
    pub fn buffer_is_allocated(&self) -> bool {
        self.buffer_capacity() > 0
    }

    /// Frees unused memory
    #[inline]
    pub fn maby_free_buffer(&mut self){
        if self.count == 0 {
            self.start = 0;
            self.buffer = vec![];
            debug_assert_eq!(self.buffer.capacity(), 0);
        }
    }


}