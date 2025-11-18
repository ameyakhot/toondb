"""
Tests for token counter utilities
"""
import unittest
import sys
import os
import importlib.util

# Import directly from files to avoid loading package __init__ which requires all dependencies
base_dir = os.path.join(os.path.dirname(__file__), '../../')
sys.path.insert(0, base_dir)

# Import token_counter
token_counter_path = os.path.join(base_dir, 'toonpy/core/token_counter.py')
spec = importlib.util.spec_from_file_location("token_counter", token_counter_path)
token_counter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(token_counter)

count_tokens = token_counter.count_tokens
count_chars = token_counter.count_chars
get_tokenizer_name = token_counter.get_tokenizer_name
get_encoding = token_counter.get_encoding
is_tiktoken_available = token_counter.is_tiktoken_available


class TestCountChars(unittest.TestCase):
    """Tests for count_chars() function"""
    
    def test_basic_counting(self):
        """Test basic character counting"""
        text = "Hello, World!"
        result = count_chars(text)
        self.assertEqual(result, 13)
    
    def test_empty_string(self):
        """Test empty string"""
        result = count_chars("")
        self.assertEqual(result, 0)
    
    def test_unicode_characters(self):
        """Test unicode characters"""
        text = "José 🚀 北京"
        result = count_chars(text)
        # Should count all characters including unicode
        self.assertGreater(result, 0)
    
    def test_multiline_string(self):
        """Test multiline string"""
        text = "Line 1\nLine 2\nLine 3"
        result = count_chars(text)
        self.assertEqual(result, 20)  # Includes newlines


class TestCountTokens(unittest.TestCase):
    """Tests for count_tokens() function"""
    
    def test_basic_token_counting(self):
        """Test basic token counting"""
        text = "Hello, World!"
        result = count_tokens(text)
        self.assertIsInstance(result, int)
        self.assertGreater(result, 0)
    
    def test_empty_string(self):
        """Test empty string"""
        result = count_tokens("")
        self.assertEqual(result, 0)
    
    def test_different_models(self):
        """Test token counting with different models"""
        text = "This is a test sentence."
        
        # Test with gpt-4
        tokens_gpt4 = count_tokens(text, model="gpt-4")
        self.assertIsInstance(tokens_gpt4, int)
        self.assertGreater(tokens_gpt4, 0)
        
        # Test with gpt-3.5-turbo
        tokens_gpt35 = count_tokens(text, model="gpt-3.5-turbo")
        self.assertIsInstance(tokens_gpt35, int)
        self.assertGreater(tokens_gpt35, 0)
    
    def test_fallback_when_tiktoken_unavailable(self):
        """Test fallback behavior when tiktoken not available"""
        # This test verifies the function doesn't crash
        # If tiktoken is not available, it should use character approximation
        text = "This is a test."
        result = count_tokens(text)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
    
    def test_long_text(self):
        """Test token counting with long text"""
        text = " ".join(["word"] * 100)
        result = count_tokens(text)
        self.assertIsInstance(result, int)
        self.assertGreater(result, 0)


class TestGetTokenizerName(unittest.TestCase):
    """Tests for get_tokenizer_name() function"""
    
    def test_gpt4(self):
        """Test gpt-4 tokenizer name"""
        result = get_tokenizer_name("gpt-4")
        self.assertEqual(result, "gpt-4")
    
    def test_gpt35_turbo(self):
        """Test gpt-3.5-turbo tokenizer name"""
        result = get_tokenizer_name("gpt-3.5-turbo")
        self.assertEqual(result, "gpt-3.5-turbo")
    
    def test_case_insensitive(self):
        """Test case insensitive matching"""
        result = get_tokenizer_name("GPT-4")
        self.assertEqual(result, "gpt-4")
    
    def test_unknown_model(self):
        """Test unknown model returns original"""
        result = get_tokenizer_name("unknown-model")
        self.assertEqual(result, "unknown-model")
    
    def test_azure_naming(self):
        """Test Azure model naming"""
        result = get_tokenizer_name("gpt-35-turbo")
        self.assertEqual(result, "gpt-3.5-turbo")


class TestGetEncoding(unittest.TestCase):
    """Tests for get_encoding() function"""
    
    def test_get_encoding_with_tiktoken(self):
        """Test getting encoding when tiktoken is available"""
        if is_tiktoken_available():
            encoding = get_encoding("gpt-4")
            self.assertIsNotNone(encoding)
        else:
            # Skip test if tiktoken not available
            self.skipTest("tiktoken not available")
    
    def test_get_encoding_raises_when_unavailable(self):
        """Test that get_encoding raises ImportError when tiktoken unavailable"""
        if not is_tiktoken_available():
            with self.assertRaises(ImportError):
                get_encoding("gpt-4")


class TestIsTiktokenAvailable(unittest.TestCase):
    """Tests for is_tiktoken_available() function"""
    
    def test_returns_boolean(self):
        """Test that function returns boolean"""
        result = is_tiktoken_available()
        self.assertIsInstance(result, bool)


if __name__ == '__main__':
    unittest.main()

