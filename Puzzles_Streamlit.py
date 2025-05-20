import streamlit as st
import chess
import chess.svg
import random
import time
import sys
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from PIL import Image as PILImage
import cairosvg
from update import ensure_latest_csv

def load_themes():
    with open('list_of_themes.txt', 'r') as f:
        return eval(f.read())

def load_openings():
    with open('list_of_openings.txt', 'r') as f:
        return [line.strip() for line in f.readlines()]

def search_puzzles(k, rating_range, themes, openings, starting_color):
    # Load parquet
    parquet_path = Path(__file__).parent / "lichess_db_puzzle.parquet"
    stream = open(parquet_path, "rb")
    
    with st.spinner('Loading puzzles...'):
        progress_bar = st.progress(0)
        
        print("Loading parquet data...")
        df = pd.read_parquet(stream, engine='pyarrow')
        progress_bar.progress(20)
        
        # Convert rating to numeric
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        
        # Apply filters
        tolerance = 50
        df = df[abs(df['rating'] - rating_range) <= tolerance]
        progress_bar.progress(40)
        
        # Filter by themes
        if themes:
            theme_list = themes.split()
            df = df[df['themes'].apply(lambda x: all(theme in x.split() for theme in theme_list))]
        progress_bar.progress(60)
        
        # Filter by openings
        if openings:
            opening_list = [opening.replace(' ', '_') for opening in openings.split("\n")]
            df = df[df['openingTags'].apply(lambda x: all(opening in x.split(',') for opening in opening_list) if isinstance(x, str) else False)]
        progress_bar.progress(80)
        
        # Filter by starting color
        if starting_color:
            df = df[~df['fen'].str.contains(f" {starting_color} ")]
        progress_bar.progress(90)
        
        total = len(df)
        df['moves_split'] = df['moves'].apply(lambda x: x.split())
        
        # Perform sampling
        if total <= k:
            selected_df = df
        else:
            selected_df = df.sample(n=k)
        
        # Convert to reservoir format
        reservoir = [(row['id'], row['fen'], row['moves_split']) 
                    for _, row in selected_df.iterrows()]
        
        progress_bar.progress(100)
        st.success(f"Found {total} puzzles matching your criteria")
        
        return reservoir

def generate_puzzle_pdf(selected):
    with st.spinner('Generating PDF...'):
        progress_bar = st.progress(0)
        
        # Import Helvetica font
        from reportlab.pdfbase.pdfmetrics import registerFontFamily
        registerFontFamily('Helvetica', normal='Helvetica', bold='Helvetica-Bold', 
                         italic='Helvetica-Oblique', boldItalic='Helvetica-BoldOblique')
        
        # Build PDF
        timestr = time.strftime("%d-%H:%M:%S")
        pdf_filename = f"Puzzle_{timestr}.pdf"
        c = canvas.Canvas(pdf_filename, pagesize=letter)
        width, height = letter
        per_page = 16
        row_offsets = [1, 3, 5, 7]
        
        total_puzzles = len(selected)
        for idx, (pid, fen, sol) in enumerate(selected, start=1):
            progress = idx / total_puzzles
            progress_bar.progress(progress)
            
            board = chess.Board(fen)
            first_move = sol[0]
            board.push_san(first_move)
            last_move = chess.Move.from_uci(sol[-1])
            
            # Generate SVG
            board_size_in = 1.5
            dpi = 300
            board_size_px = board_size_in * dpi
            board_size_pt = board_size_in * inch
            
            custom_colors = {
                "square light": "#ffffff",
                "square dark": "#adadad",
            }
            
            orientation = board.turn
            move = sol[0]
            tail = move[0:2]
            head = move[2:4]
            arrow = [chess.svg.Arrow(chess.parse_square(tail), chess.parse_square(head), color="#00000040")]
            
            svg_content = chess.svg.board(
                board=board,
                arrows=arrow,
                coordinates=True,
                orientation=orientation,
                size=board_size_px,
                colors=custom_colors
            )
            
            # Write SVG to temporary file
            temp_svg_path = Path("temp_board.svg")
            with open(temp_svg_path, "w") as f:
                f.write(svg_content)
            
            # Convert SVG to PNG
            png_filename = f"temp_board_{idx}.png"
            cairosvg.svg2png(url=str(temp_svg_path), write_to=png_filename, 
                           output_width=board_size_px, output_height=board_size_px, dpi=dpi)
            image_draw = Path(png_filename)
            
            # Calculate position
            slot = (idx-1) % per_page
            if slot == 0 and idx != 1:
                c.showPage()
            col = slot % 4
            row = slot // 4
            x_origin = .75 * inch + col * (board_size_pt * 1.2)
            y_origin = height - row_offsets[row] * inch - 1.5 * inch
            
            # Draw board
            c.drawImage(image_draw, x_origin, y_origin, width=board_size_pt, height=board_size_pt, 
                       preserveAspectRatio=True)
            
            # Clean up
            temp_svg_path.unlink()
            image_draw.unlink()
            
            # Add puzzle number and ID
            c.setFont("Helvetica", 10)
            c.drawString(x_origin, y_origin + board_size_pt + 5, f"Puzzle {idx}  (ID={pid})")
        
        # Solution page
        c.showPage()
        c.setFont("Helvetica", 10)
        c.drawString(inch * 0.5, height - inch, "Solutions")
        c.setFont("Helvetica", 8)
        y = height - inch * 1.2
        
        for i, (pid, fen, sol) in enumerate(selected, start=1):
            board = chess.Board(fen)
            moves_texts = []
            for uci in sol:
                move = chess.Move.from_uci(uci)
                san = board.san(move)
                moves_texts.append(san)
                board.push(move)
            moves_line = " ".join(moves_texts[1:])
            c.drawString(inch * 0.5, y, f"Puzzle {i}: {moves_line}")
            y -= 14
            if y < inch:
                c.showPage()
                c.setFont("Helvetica", 10)
                c.drawString(inch * 0.5, height - inch, "Solutions (continued)")
                c.setFont("Helvetica", 8)
                y = height - inch * 1.2
        
        c.save()
        st.success("PDF generation complete!")
        return pdf_filename

def main():
    st.set_page_config(page_title="Chess Puzzle Generator", layout="wide")
    st.title("Chess Puzzle Generator")
    
    # Load data
    themes_dict = load_themes()
    openings_list = load_openings()
    
    # Input fields
    col1, col2 = st.columns(2)
    with col1:
        k = st.number_input("Number of Puzzles (1-100):", min_value=1, max_value=100, value=25)
    with col2:
        rating_range = st.number_input("Rating Range (300-3000):", min_value=300, max_value=3000, value=1500)
    
    # Themes selection
    st.subheader("Select Themes")
    selected_themes = []

    # Create columns for different categories (horizontal layout)
    categories = {
        'Game State': ['crushing', 'advantage', 'equality', 'opening', 'middlegame', 'endgame'],
        'Puzzle Length': ['oneMove', 'short', 'long', 'veryLong'],
        'Mate Patterns': ['mate', 'mateIn1', 'mateIn2', 'mateIn3', 'mateIn4', 'mateIn5', 
                        'backRankMate', 'bodenMate', 'hookMate', 'arabianMate', 
                        'dovetailMate', 'smotheredMate', 'anastasiaMate', 'doubleBishopMate'],
        'Tactical Themes': ['fork', 'pin', 'skewer', 'discoveredAttack', 'xRayAttack',
                          'doubleCheck', 'deflection', 'interference', 'clearance',
                          'attraction', 'capturingDefender', 'hangingPiece', 'trappedPiece', 'sacrifice', 
                          'intermezzo'],
        'Positional Themes': ['advancedPawn', 'exposedKing', 'kingsideAttack', 'queensideAttack',
                            'attackingF2F7', 'quietMove', 'defensiveMove', 'zugzwang'],
        'Endgame Types': ['pawnEndgame', 'knightEndgame', 'bishopEndgame', 'rookEndgame',
                        'queenEndgame', 'queenRookEndgame'],
        'Special Moves': ['enPassant', 'castling', 'underPromotion', 'promotion'],
        'Player Level': ['master', 'masterVsMaster', 'superGM'],
    }

    # Create a column for each category
    theme_columns = st.columns(len(categories))
    for col, (category, themes) in zip(theme_columns, categories.items()):
        with col:
            st.markdown(f"**{themes_dict.get(category.lower(), category)}**")
            for theme in themes:
                display_name = themes_dict.get(theme, theme)
                if st.checkbox(display_name, key=theme):
                    selected_themes.append(theme)
    
    # Openings selection
    st.subheader("Openings")
    selected_openings = st.multiselect(
        "Select openings (type to search):",
        options=openings_list,
        default=None
    )
    
    # Starting color
    st.subheader("Starting Color")
    starting_color = st.radio(
        "Choose starting color:",
        options=["Either", "White", "Black"],
        horizontal=True
    )
    starting_color = starting_color[0].lower() if starting_color != "Either" else ""
    
    # Generate button
    if st.button("Generate Puzzles"):
        if not 1 <= k <= 100:
            st.error("Number of puzzles must be between 1 and 100")
            return
        if not 300 <= rating_range <= 3000:
            st.error("Rating must be between 300 and 3000")
            return
            
        themes = " ".join(selected_themes) if selected_themes else ""
        openings = "\n".join(selected_openings) if selected_openings else ""
        
        selected = search_puzzles(k, rating_range, themes, openings, starting_color)
        if selected:
            pdf_filename = generate_puzzle_pdf(selected)
            with open(pdf_filename, "rb") as f:
                st.download_button(
                    label="Download PDF",
                    data=f,
                    file_name=pdf_filename,
                    mime="application/pdf"
                )

if __name__ == "__main__":
    ensure_latest_csv()
    main() 