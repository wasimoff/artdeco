use std::collections::VecDeque;
use anyhow::{anyhow, Result};
use bytesize::ByteSize;

/// Maximum fragment size in bytes (configurable constant)
pub const MAX_FRAGMENT_SIZE: usize = ByteSize::kb(64).as_u64() as usize;

/// Represents a complete message ready to be processed
#[derive(Debug, Clone)]
pub struct CompleteMessage {
    pub data: Vec<u8>,
}

/// Represents a fragment of a message
#[derive(Debug, Clone)]
pub struct Fragment {
    pub data: Vec<u8>,
}

/// Fragment a message into multiple packets with the default fragment size
pub fn fragment_message(data: &[u8]) -> Vec<Fragment> {
    fragment_message_with_size(data, MAX_FRAGMENT_SIZE)
}

/// Fragment a message into multiple packets with a custom fragment size
pub fn fragment_message_with_size(data: &[u8], fragment_size: usize) -> Vec<Fragment> {
    if fragment_size < 5 {  // Need at least 4 bytes for length + 1 byte for data
        panic!("Fragment size must be at least 5 bytes");
    }

    let mut fragments = Vec::new();
    
    // First fragment contains the length prefix
    let length_bytes = (data.len() as u32).to_be_bytes();
    let first_fragment_data_size = fragment_size - 4; // Reserve 4 bytes for length
    
    if data.len() <= first_fragment_data_size {
        // Message fits in a single fragment
        let mut fragment_data = Vec::with_capacity(4 + data.len());
        fragment_data.extend_from_slice(&length_bytes);
        fragment_data.extend_from_slice(data);
        fragments.push(Fragment { data: fragment_data });
    } else {
        // Message needs multiple fragments
        let mut remaining_data = data;
        
        // First fragment with length prefix
        let mut first_fragment = Vec::with_capacity(fragment_size);
        first_fragment.extend_from_slice(&length_bytes);
        first_fragment.extend_from_slice(&remaining_data[..first_fragment_data_size]);
        fragments.push(Fragment { data: first_fragment });
        
        remaining_data = &remaining_data[first_fragment_data_size..];
        
        // Subsequent fragments
        while !remaining_data.is_empty() {
            let chunk_size = remaining_data.len().min(fragment_size);
            fragments.push(Fragment {
                data: remaining_data[..chunk_size].to_vec(),
            });
            remaining_data = &remaining_data[chunk_size..];
        }
    }
    
    fragments
}

/// State machine for packet defragmentation
#[derive(Debug)]
pub enum DefragmentationState {
    /// Waiting for a new message (first 4 bytes must contain the length)
    WaitingForNewMessage,
    /// Waiting for message data
    WaitingForData { expected_length: u32, buffer: Vec<u8> },
}

/// Packet defragmenter that buffers incoming bytes and reconstructs complete messages
#[derive(Debug)]
pub struct PacketDefragmenter {
    state: DefragmentationState,
    completed_messages: VecDeque<CompleteMessage>,
}

impl PacketDefragmenter {
    /// Create a new packet defragmenter
    pub fn new() -> Self {
        Self {
            state: DefragmentationState::WaitingForNewMessage,
            completed_messages: VecDeque::new(),
        }
    }

    /// Process incoming bytes and potentially produce complete messages
    pub fn process_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let mut offset = 0;

        while offset < bytes.len() {
            match &mut self.state {
                DefragmentationState::WaitingForNewMessage => {
                    // First bytes must be at least 4 bytes to contain the length
                    let remaining_bytes = bytes.len() - offset;
                    if remaining_bytes < 4 {
                        return Err(anyhow!("First message bytes must be at least 4 bytes to parse length, got {}", remaining_bytes));
                    }

                    // Read the length from the first 4 bytes
                    let length_bytes: [u8; 4] = bytes[offset..offset + 4].try_into()
                        .map_err(|_| anyhow!("Failed to convert length bytes"))?;
                    let expected_length = u32::from_be_bytes(length_bytes);
                    offset += 4;

                    // Validate message length (prevent excessive memory allocation)
                    if expected_length > 100_000_000 {  // 100MB limit
                        return Err(anyhow!("Message too large: {} bytes", expected_length));
                    }

                    // Special case for zero-length messages
                    if expected_length == 0 {
                        self.completed_messages.push_back(CompleteMessage {
                            data: Vec::new(),
                        });
                        // Reset state for next message
                        self.state = DefragmentationState::WaitingForNewMessage;
                    } else {
                        self.state = DefragmentationState::WaitingForData {
                            expected_length,
                            buffer: Vec::with_capacity(expected_length as usize),
                        };
                    }
                }
                DefragmentationState::WaitingForData { expected_length, buffer } => {
                    let bytes_needed = *expected_length as usize - buffer.len();
                    let bytes_available = bytes.len() - offset;
                    let bytes_to_copy = bytes_needed.min(bytes_available);

                    buffer.extend_from_slice(&bytes[offset..offset + bytes_to_copy]);
                    offset += bytes_to_copy;

                    // If we have the complete message, add it to completed messages
                    if buffer.len() == *expected_length as usize {
                        self.completed_messages.push_back(CompleteMessage {
                            data: buffer.clone(),
                        });

                        // Reset state for next message
                        self.state = DefragmentationState::WaitingForNewMessage;
                    }
                }
            }
        }

        Ok(())
    }

    /// Get the next complete message if available
    pub fn next_message(&mut self) -> Option<CompleteMessage> {
        self.completed_messages.pop_front()
    }
}

impl Default for PacketDefragmenter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_fragment_message() {
        let mut defragmenter = PacketDefragmenter::new();

        let original_data = b"Hello, World!";
        let fragments = fragment_message(original_data);
        
        assert_eq!(fragments.len(), 1);
        
        defragmenter.process_bytes(&fragments[0].data).unwrap();
        
        let message = defragmenter.next_message().unwrap();
        assert_eq!(message.data, original_data);
    }

    #[test]
    fn test_multi_fragment_message() {
        let mut defragmenter = PacketDefragmenter::new();

        let original_data = b"This is a longer message that will be split into multiple fragments";
        let fragments = fragment_message_with_size(original_data, 10); // Small fragments for testing
        
        assert!(fragments.len() > 1);
        
        // Process all fragments
        for fragment in fragments {
            defragmenter.process_bytes(&fragment.data).unwrap();
        }
        
        let message = defragmenter.next_message().unwrap();
        assert_eq!(message.data, original_data);
    }

    #[test]
    fn test_multiple_messages() {
        let mut defragmenter = PacketDefragmenter::new();

        let messages = vec![b"First message".as_slice(), b"Second message", b"Third"];
        
        // Fragment and process all messages
        for msg in &messages {
            let fragments = fragment_message(msg);
            for fragment in fragments {
                defragmenter.process_bytes(&fragment.data).unwrap();
            }
        }
        
        // Verify all messages were received
        for expected_msg in messages {
            let received_msg = defragmenter.next_message().unwrap();
            assert_eq!(received_msg.data, expected_msg);
        }
    }

    #[test]
    fn test_empty_message() {
        let mut defragmenter = PacketDefragmenter::new();

        let original_data = b"";
        let fragments = fragment_message(original_data);
        
        assert_eq!(fragments.len(), 1);
        
        defragmenter.process_bytes(&fragments[0].data).unwrap();
        
        let message = defragmenter.next_message().unwrap();
        assert_eq!(message.data, original_data);
    }

    #[test]
    #[should_panic(expected = "Fragment size must be at least 5 bytes")]
    fn test_invalid_fragment_size() {
        fragment_message_with_size(b"test", 4);
    }

    #[test]
    fn test_large_message_validation() {
        let mut defragmenter = PacketDefragmenter::new();
        
        // Try to process a message with invalid length
        let invalid_length = 200_000_000u32; // Larger than 100MB limit
        let length_bytes = invalid_length.to_be_bytes();
        
        let result = defragmenter.process_bytes(&length_bytes);
        assert!(result.is_err());
    }
}