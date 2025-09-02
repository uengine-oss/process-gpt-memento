#!/usr/bin/env python3
"""
Markdown to HTML converter with multiple Mermaid rendering options
Usage: python3 md2html_converter.py <input.md> [--output output.html] [--mode MODE]
"""

import argparse
import sys
import os
from convert_to_html import (
    convert_md_to_html_with_mermaid,
    create_dynamic_mermaid_html, 
    convert_md_to_html_with_inline_svg
)

def main():
    parser = argparse.ArgumentParser(
        description="Convert Markdown files with Mermaid diagrams to HTML",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Rendering modes:
  static    - Use pre-generated SVG files (requires mmdc to generate SVGs first)
  dynamic   - Use Mermaid.js for browser-side rendering (requires internet)
  inline    - Embed SVG directly in HTML (self-contained, no external files)

Examples:
  python3 md2html_converter.py sample.md --mode inline
  python3 md2html_converter.py sample.md --output result.html --mode dynamic
  python3 md2html_converter.py sample_converted.md --mode static
        """
    )
    
    parser.add_argument('input', help='Input Markdown file')
    parser.add_argument('--output', '-o', help='Output HTML file (default: input filename with .html extension)')
    parser.add_argument('--mode', '-m', 
                       choices=['static', 'dynamic', 'inline'],
                       default='inline',
                       help='Rendering mode (default: inline)')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input):
        print(f"Error: Input file '{args.input}' not found!")
        sys.exit(1)
    
    # Generate output filename if not provided
    if args.output is None:
        base_name = os.path.splitext(args.input)[0]
        args.output = f"{base_name}_{args.mode}.html"
    
    # Convert based on mode
    try:
        if args.mode == 'static':
            print(f"Converting {args.input} to {args.output} using static SVG mode...")
            convert_md_to_html_with_mermaid(args.input, args.output)
            
        elif args.mode == 'dynamic':
            print(f"Converting {args.input} to {args.output} using dynamic Mermaid.js mode...")
            create_dynamic_mermaid_html(args.input, args.output)
            
        elif args.mode == 'inline':
            print(f"Converting {args.input} to {args.output} using inline SVG mode...")
            convert_md_to_html_with_inline_svg(args.input, args.output)
        
        print(f"✅ Conversion completed successfully!")
        print(f"📁 Output file: {args.output}")
        
        # Show file size
        file_size = os.path.getsize(args.output)
        print(f"📊 File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
