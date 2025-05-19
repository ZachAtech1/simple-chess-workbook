import chess , chess.svg
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
from update import ensure_latest_csv
from PIL import Image as PILImage
import cairosvg
# check for latest CSV
ensure_latest_csv()

# Import Helvetica font
from reportlab.pdfbase.pdfmetrics import registerFontFamily
registerFontFamily('Helvetica', normal='Helvetica', bold='Helvetica-Bold', italic='Helvetica-Oblique', boldItalic='Helvetica-BoldOblique')

# Load parquetx
parquet_path = Path(__file__).parent / "lichess_db_puzzle.parquet"
stream=open(parquet_path, "rb")


# Use reservoir sampling to select user number random puzzles
k = int(input("How many puzzles do you want? "))
while not k in range(1,100):
    if not k in range(1,100):
        print("Puzzle range is 1-100")
        k=int(input("How many puzzles do you want? "))
    else:
        break

print("Begin reservoir sampling .... ")
reservoir = []
total = 0

# Ask user for input values
rating_range = int(input("What rating are you looking for? (300 - 3000) "))
themes = input("What themes are you looking for? (empty if none) ")
openings = input("What openings are you looking for? (empty if none) ")
starting_color = input("What starting color are you looking for? (b/w or empty for either) ")
tolerance = 50
# Ensure rating range
while not rating_range in range(300, 3000):
    if not rating_range in range(300, 300):
        print("Please enter a rating between 300 and 3000")
        rating_range=int(input("What rating are you looking for (300 - 3000) "))
    else:
        break

# Start timer for processing time
start_time = time.time() 

print("Loading parquet data...")
df = pd.read_parquet(stream, engine='pyarrow') # pyarrow makes this 10x faster

with tqdm(total=4, desc="Processing data", unit="step", ncols=80) as pbar: # use tqdm to track progress
    
    # Convert rating to numeric
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    pbar.update(1)
    
    # Apply filters
    # Filter by rating
    df = df[abs(df['rating'] - rating_range) <= tolerance]
    
    # Filter by themes
    if themes:
        theme_list = themes.split()
        df = df[df['themes'].apply(lambda x: all(theme in x.split() for theme in theme_list))]
    pbar.update(1)
    
    # Filter by openings
    if openings:
        # Take the opening list and replace spaces with underscores as they are used in the parquet file
        opening_list = [opening.replace(' ', '_') for opening in openings.split(",")]
        df = df[df['openingTags'].apply(lambda x: all(opening in x.split(',') for opening in opening_list) if isinstance(x, str) else False)]
        print(opening_list)

    # Filter by starting color
    if starting_color:
        df = df[~df['fen'].str.contains(f" {starting_color} ")] 
    pbar.update(1)
    
    total = len(df) # count the number of puzzles
    
    df['moves_split'] = df['moves'].apply(lambda x: x.split()) # split the solutions into a list
    
    # Perform reservoir sampling
    
    if total <= k:
        selected_df = df # take all if we have fewer than k puzzles
    else:
        selected_df = df.sample(n=k)
    
    # Convert to the format needed for the reservoir
    reservoir = [(row['id'], row['fen'], row['moves_split']) 
                for _, row in selected_df.iterrows()]
    pbar.update(1)

end_time = time.time()

print(f"Time taken: {end_time - start_time:.5f} seconds")

print(f"\nFound {total} puzzles matching your rating criteria")
print(f"Selected {len(reservoir)} puzzles")
print("\nGenerating PDF...")

# shuffle to randomize order on output
random.shuffle(reservoir)
selected = reservoir
# Build PDF with max 16 puzzles per page
timestr = time.strftime("%d-%H:%M:%S")
pdf_filename = f"Puzzle_{timestr}.pdf"
c = canvas.Canvas(pdf_filename, pagesize=letter)
width, height = letter
per_page = 16
# row offsets for each row in inches
row_offsets = [1, 3, 5, 7] 

'''
 Lichess provides puzzles where the FEN represents the position before the first move.
    In order to fix this we need to apply the first move to the board and update the FEN.
    This is done by using the chess library to create a board object from the FEN string,
'''

print("Starting PDF generation...")
for idx, (pid, fen, sol) in enumerate(tqdm(selected, desc="Generating PDF", unit=" puzzles", ncols=80, file=sys.stdout), start=1):
    board = chess.Board(fen)
    first_move = sol[0]
    board.push_san(first_move)
    last_move = chess.Move.from_uci(sol[-1])
    
    # Generate SVG for this board
    board_size_in = 1.5
    dpi = 300
    board_size_px = board_size_in * dpi # define the board size in pixels
    board_size_pt = board_size_in * inch
    
    custom_colors={
        "square light": "#ffffff",
        "square dark" : "#adadad",
    }
    
    # Set orientation based on whose turn it is to move in the puzzle position
    orientation = board.turn  # True for white to move, False for black to move
    
    # Split the move into source and target squares
    move = sol[0]
    tail = move[:2]  # First two characters
    head = move[2:]  # Last two characters
    arrow = [chess.svg.Arrow(chess.parse_square(tail), chess.parse_square(head), color="#000000")]
    svg_content = chess.svg.board(
        board=board,
        arrows=arrow,
        coordinates=True,
        orientation=orientation,
        size=board_size_px,  # adjust size to fit your layout
        colors=custom_colors
    )
    
    
    # Write SVG to a temporary file
    temp_svg_path = Path("temp_board.svg")
    with open(temp_svg_path, "w") as f:
        f.write(svg_content)
    
    
    # Convert svg to png with cairosvg
    png_filename = f"temp_board_{idx}.png"
    cairosvg.svg2png(url=str(temp_svg_path), write_to=png_filename, output_width=board_size_px, output_height=board_size_px, dpi=dpi)
    image_draw = Path(png_filename)
    # Calculate position on page (using your existing layout logic)
    slot = (idx-1) % per_page
    if slot == 0 and idx != 1:
        c.showPage()
    col = slot % 4
    row = slot // 4
    x_origin =  .75 * inch + col * (board_size_pt * 1.2)
    y_origin = height - row_offsets[row] * inch - 1.5 * inch
    
    # Draw the board image at the calculated position
    c.drawImage(image_draw, x_origin, y_origin, width=board_size_pt, height=board_size_pt, preserveAspectRatio=True)

    #Clean up temporary files
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

# For the solution lichess gives in simple text (e1d2) we use the chess library
# to convert the UCI move to SAN (e.g. e1d2 -> Kd2)
for i, (pid, fen, sol) in enumerate(selected, start=1):
    # apply first move and update board for SAN conversion
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
        y = height - inch

c.save()
# testing