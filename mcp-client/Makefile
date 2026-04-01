.PHONY: install setup test-connection

install:
	poetry install

setup: install
	@echo ""
	@echo "MrDocument client installed."
	@echo ""
	@echo "Add to your shell profile (.bashrc / .zshrc):"
	@echo ""
	@echo "  export MRDOCUMENT_USER=your_username"
	@echo "  export MRDOCUMENT_PASSWORD_FILE=/path/to/.mcp-password"
	@echo ""
	@echo "Then run 'claude' from this directory."
	@echo ""

test-connection:
	poetry run python3 test_connection.py
