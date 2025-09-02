#!/usr/bin/env python3
"""
Markdown to HTML converter with Mermaid support
"""

import re
import markdown
import subprocess
import tempfile
import os
from pathlib import Path

def generate_inline_svg_from_mermaid(mermaid_code):
    """
    Generate SVG content from Mermaid code using mermaid-cli
    Returns the SVG content as a string
    """
    try:
        # Create a temporary file for the mermaid code
        with tempfile.NamedTemporaryFile(mode='w', suffix='.mmd', delete=False) as temp_mmd:
            temp_mmd.write(mermaid_code)
            temp_mmd_path = temp_mmd.name
        
        # Create temporary SVG file path
        temp_svg_path = temp_mmd_path.replace('.mmd', '.svg')
        
        # Run mermaid-cli to generate SVG
        result = subprocess.run([
            'mmdc', '-i', temp_mmd_path, '-o', temp_svg_path, 
            '--backgroundColor', 'transparent'
        ], capture_output=True, text=True)
        
        if result.returncode == 0 and os.path.exists(temp_svg_path):
            # Read the generated SVG content
            with open(temp_svg_path, 'r', encoding='utf-8') as svg_file:
                svg_content = svg_file.read()
            
            # Clean up temporary files
            os.unlink(temp_mmd_path)
            os.unlink(temp_svg_path)
            
            return svg_content
        else:
            print(f"Error generating SVG: {result.stderr}")
            return f'<div class="error">Failed to generate diagram</div>'
            
    except Exception as e:
        print(f"Exception in SVG generation: {e}")
        return f'<div class="error">Error: {e}</div>'

def convert_md_to_html_with_inline_svg(md_file_path, output_file_path):
    """
    Convert Markdown to HTML with inline SVG embedded directly in HTML
    """
    
    # Read the original markdown file
    with open(md_file_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Function to replace mermaid blocks with inline SVG
    def replace_mermaid_with_svg(match):
        mermaid_code = match.group(1).strip()
        svg_content = generate_inline_svg_from_mermaid(mermaid_code)
        
        # Wrap SVG in a container div
        return f'<div class="mermaid-container">\n{svg_content}\n</div>'
    
    # Replace ```mermaid blocks with inline SVG
    md_content = re.sub(r'```mermaid\n(.*?)\n```', replace_mermaid_with_svg, md_content, flags=re.DOTALL)
    
    # Convert remaining Markdown to HTML
    md = markdown.Markdown(extensions=[
        'extra',
        'codehilite', 
        'toc',
        'fenced_code',
        'tables'
    ])
    
    html_content = md.convert(md_content)
    
    # Create complete HTML document
    html_template = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>대규모 딥러닝 시스템 아키텍처 설계 문서 (Inline SVG)</title>
    
    <!-- MathJax for mathematical expressions -->
    <script src="https://polyfill.io/v3/polyfill.min.js?features=es6"></script>
    <script id="MathJax-script" async src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>
    <script>
        window.MathJax = {{
            tex: {{
                inlineMath: [['$', '$'], ['\\\\(', '\\\\)']],
                displayMath: [['$$', '$$'], ['\\\\[', '\\\\]']]
            }}
        }};
    </script>
    
    <!-- Prism.js for syntax highlighting -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-yaml.min.js"></script>
    
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
        }}
        
        h1, h2, h3, h4, h5, h6 {{
            color: #2c3e50;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }}
        
        h1 {{
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        
        h2 {{
            border-bottom: 2px solid #e74c3c;
            padding-bottom: 8px;
        }}
        
        h3 {{
            border-bottom: 1px solid #95a5a6;
            padding-bottom: 5px;
        }}
        
        code {{
            background-color: #f4f4f4;
            padding: 2px 4px;
            border-radius: 3px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        }}
        
        pre {{
            background-color: #f8f8f8;
            border: 1px solid #e1e1e8;
            border-radius: 5px;
            padding: 1rem;
            overflow-x: auto;
        }}
        
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1rem 0;
        }}
        
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        
        .mermaid-container {{
            text-align: center;
            margin: 2rem 0;
            padding: 1rem;
            background-color: #fafafa;
            border: 1px solid #e1e1e8;
            border-radius: 8px;
        }}
        
        .mermaid-container svg {{
            max-width: 100%;
            height: auto;
        }}
        
        .error {{
            color: #e74c3c;
            background-color: #fdf2f2;
            border: 1px solid #f5c6cb;
            border-radius: 4px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        img {{
            max-width: 100%;
            height: auto;
            display: block;
            margin: 1rem auto;
            border: 1px solid #ddd;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        
        blockquote {{
            border-left: 4px solid #3498db;
            margin: 1rem 0;
            padding: 0.5rem 1rem;
            background-color: #f8f9fa;
        }}
        
        .toc {{
            background-color: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 5px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        hr {{
            border: none;
            height: 2px;
            background: linear-gradient(to right, #3498db, #e74c3c);
            margin: 2rem 0;
        }}
        
        .math {{
            overflow-x: auto;
        }}
        
        /* Responsive design */
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
            
            h1 {{
                font-size: 1.8rem;
            }}
            
            h2 {{
                font-size: 1.5rem;
            }}
            
            h3 {{
                font-size: 1.3rem;
            }}
            
            .mermaid-container {{
                padding: 0.5rem;
            }}
        }}
    </style>
</head>
<body>
    {html_content}
</body>
</html>"""
    
    # Write the HTML file
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
    
    print(f"Inline SVG HTML file created: {output_file_path}")

def convert_md_to_html_with_mermaid(md_file_path, output_file_path):
    """
    Convert Markdown file to HTML with embedded Mermaid diagrams and math support
    """
    
    # Read the markdown file
    with open(md_file_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Convert Markdown to HTML using python-markdown
    md = markdown.Markdown(extensions=[
        'extra',           # Tables, code blocks, etc.
        'codehilite',      # Syntax highlighting
        'toc',             # Table of contents
        'fenced_code',     # Fenced code blocks
        'tables'           # Table support
    ])
    
    html_content = md.convert(md_content)
    
    # Create a complete HTML document
    html_template = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>대규모 딥러닝 시스템 아키텍처 설계 문서</title>
    
    <!-- MathJax for mathematical expressions -->
    <script src="https://polyfill.io/v3/polyfill.min.js?features=es6"></script>
    <script id="MathJax-script" async src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>
    <script>
        window.MathJax = {{
            tex: {{
                inlineMath: [['$', '$'], ['\\\\(', '\\\\)']],
                displayMath: [['$$', '$$'], ['\\\\[', '\\\\]']]
            }}
        }};
    </script>
    
    <!-- Mermaid for diagrams -->
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{ 
            startOnLoad: true,
            theme: 'default',
            securityLevel: 'loose',
            flowchart: {{
                useMaxWidth: true,
                htmlLabels: true
            }}
        }});
    </script>
    
    <!-- Prism.js for syntax highlighting -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-yaml.min.js"></script>
    
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
        }}
        
        h1, h2, h3, h4, h5, h6 {{
            color: #2c3e50;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }}
        
        h1 {{
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        
        h2 {{
            border-bottom: 2px solid #e74c3c;
            padding-bottom: 8px;
        }}
        
        h3 {{
            border-bottom: 1px solid #95a5a6;
            padding-bottom: 5px;
        }}
        
        code {{
            background-color: #f4f4f4;
            padding: 2px 4px;
            border-radius: 3px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        }}
        
        pre {{
            background-color: #f8f8f8;
            border: 1px solid #e1e1e8;
            border-radius: 5px;
            padding: 1rem;
            overflow-x: auto;
        }}
        
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1rem 0;
        }}
        
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        
        .mermaid {{
            text-align: center;
            margin: 1rem 0;
        }}
        
        img {{
            max-width: 100%;
            height: auto;
            display: block;
            margin: 1rem auto;
            border: 1px solid #ddd;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        
        blockquote {{
            border-left: 4px solid #3498db;
            margin: 1rem 0;
            padding: 0.5rem 1rem;
            background-color: #f8f9fa;
        }}
        
        .toc {{
            background-color: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 5px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        hr {{
            border: none;
            height: 2px;
            background: linear-gradient(to right, #3498db, #e74c3c);
            margin: 2rem 0;
        }}
        
        .math {{
            overflow-x: auto;
        }}
        
        /* Responsive design */
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
            
            h1 {{
                font-size: 1.8rem;
            }}
            
            h2 {{
                font-size: 1.5rem;
            }}
            
            h3 {{
                font-size: 1.3rem;
            }}
        }}
    </style>
</head>
<body>
    {html_content}
</body>
</html>"""
    
    # Write the HTML file
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
    
    print(f"HTML file created: {output_file_path}")

def create_dynamic_mermaid_html(md_file_path, output_file_path):
    """
    Create HTML with dynamic Mermaid rendering (original markdown with mermaid blocks)
    """
    
    # Read the original markdown file (not the converted one)
    original_md_path = md_file_path.replace('_converted', '')
    with open(original_md_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Convert mermaid code blocks to HTML div elements
    def replace_mermaid_blocks(match):
        mermaid_code = match.group(1)
        return f'<div class="mermaid">\n{mermaid_code}\n</div>'
    
    # Replace ```mermaid blocks with <div class="mermaid">
    md_content = re.sub(r'```mermaid\n(.*?)\n```', replace_mermaid_blocks, md_content, flags=re.DOTALL)
    
    # Convert Markdown to HTML
    md = markdown.Markdown(extensions=[
        'extra',
        'codehilite',
        'toc',
        'fenced_code',
        'tables'
    ])
    
    html_content = md.convert(md_content)
    
    # Create HTML with Mermaid.js
    html_template = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>대규모 딥러닝 시스템 아키텍처 설계 문서 (Dynamic Mermaid)</title>
    
    <!-- MathJax for mathematical expressions -->
    <script src="https://polyfill.io/v3/polyfill.min.js?features=es6"></script>
    <script id="MathJax-script" async src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-mml-chtml.js"></script>
    <script>
        window.MathJax = {{
            tex: {{
                inlineMath: [['$', '$'], ['\\\\(', '\\\\)']],
                displayMath: [['$$', '$$'], ['\\\\[', '\\\\]']]
            }}
        }};
    </script>
    
    <!-- Mermaid for diagrams -->
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{ 
            startOnLoad: true,
            theme: 'default',
            securityLevel: 'loose',
            flowchart: {{
                useMaxWidth: true,
                htmlLabels: true
            }},
            pie: {{
                useWidth: 600
            }},
            timeline: {{
                useMaxWidth: true
            }}
        }});
    </script>
    
    <!-- Prism.js for syntax highlighting -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-yaml.min.js"></script>
    
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
        }}
        
        h1, h2, h3, h4, h5, h6 {{
            color: #2c3e50;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }}
        
        h1 {{
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        
        h2 {{
            border-bottom: 2px solid #e74c3c;
            padding-bottom: 8px;
        }}
        
        h3 {{
            border-bottom: 1px solid #95a5a6;
            padding-bottom: 5px;
        }}
        
        code {{
            background-color: #f4f4f4;
            padding: 2px 4px;
            border-radius: 3px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        }}
        
        pre {{
            background-color: #f8f8f8;
            border: 1px solid #e1e1e8;
            border-radius: 5px;
            padding: 1rem;
            overflow-x: auto;
        }}
        
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1rem 0;
        }}
        
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        
        .mermaid {{
            text-align: center;
            margin: 2rem 0;
            padding: 1rem;
            background-color: #fafafa;
            border: 1px solid #e1e1e8;
            border-radius: 8px;
        }}
        
        img {{
            max-width: 100%;
            height: auto;
            display: block;
            margin: 1rem auto;
            border: 1px solid #ddd;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        
        blockquote {{
            border-left: 4px solid #3498db;
            margin: 1rem 0;
            padding: 0.5rem 1rem;
            background-color: #f8f9fa;
        }}
        
        .toc {{
            background-color: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 5px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        hr {{
            border: none;
            height: 2px;
            background: linear-gradient(to right, #3498db, #e74c3c);
            margin: 2rem 0;
        }}
        
        .math {{
            overflow-x: auto;
        }}
        
        /* Responsive design */
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
            
            h1 {{
                font-size: 1.8rem;
            }}
            
            h2 {{
                font-size: 1.5rem;
            }}
            
            h3 {{
                font-size: 1.3rem;
            }}
            
            .mermaid {{
                padding: 0.5rem;
            }}
        }}
    </style>
</head>
<body>
    {html_content}
</body>
</html>"""
    
    # Write the HTML file
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(html_template)
    
    print(f"Dynamic Mermaid HTML file created: {output_file_path}")

if __name__ == "__main__":
    # Convert the SVG-embedded version (using pre-generated SVG files)
    convert_md_to_html_with_mermaid(
        "/Users/uengine/ md2html/sample_converted.md",
        "/Users/uengine/ md2html/sample_static.html"
    )
    
    # Create dynamic Mermaid version (browser renders Mermaid)
    create_dynamic_mermaid_html(
        "/Users/uengine/ md2html/sample_converted.md",
        "/Users/uengine/ md2html/sample_dynamic.html"
    )
    
    # Create inline SVG version (SVG embedded directly in HTML)
    convert_md_to_html_with_inline_svg(
        "/Users/uengine/ md2html/sample.md",
        "/Users/uengine/ md2html/sample_inline.html"
    )
