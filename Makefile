.PHONY: format format-check tidy tidy-check help

# Help target
help:
	@echo "Available targets:"
	@echo "  format          - Auto-format all code files"
	@echo "  format-check    - Check code formatting (for CI)"
	@echo "  tidy            - Run clang-tidy checks"
	@echo "  tidy-check      - Run clang-tidy checks (alias for tidy)"

# Format targets - format默认直接格式化
format:
	@python3 scripts/format.py --all --fix --noconfirm

format-check:
	@python3 scripts/format.py --all --check

# Tidy targets
tidy:
	@if [ ! -d "build" ]; then \
		echo "Error: build directory not found. Please run cmake first."; \
		exit 1; \
	fi
	@if [ ! -f "build/compile_commands.json" ]; then \
		echo "Error: compile_commands.json not found. Please build the project first."; \
		exit 1; \
	fi
	@echo "Running clang-tidy checks..."
	@python3 scripts/run-clang-tidy.py -p build

tidy-check: tidy