.PHONY: install setup test-connection run

install:
	poetry install

run: install
	poetry run claude --append-system-prompt-file system-prompt.txt

setup: install
	@echo ""
	@echo "MrDocument client installed."
	@echo ""
	@echo "Add to your shell profile (.bashrc / .zshrc):"
	@echo ""
	@echo "  export MRDOCUMENT_USER=your_username"
	@echo "  export MRDOCUMENT_PASSWORD_FILE=/path/to/.mcp-password"
	@echo ""
	@echo "Then run 'make run' from this directory."
	@echo ""

test-connection:
	poetry run python3 test_connection.py
