import chess, csv, random, time, bz2
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from update import ensure_latest_csv
from tqdm import tqdm
import sys


# 0) check for latest CSV
ensure_latest_csv()

# register Apple Symbols font (present on all macOS systems) for chess glyphs
apple_symbols_paths = [
    Path("/System/Library/Fonts/Apple Symbols.ttf"),
    Path("/System/Library/Fonts/Supplemental/Apple Symbols.ttf")
]
for font_path in apple_symbols_paths:
    if font_path.exists():
        pdfmetrics.registerFont(TTFont("AppleSymbols", str(font_path)))
        break
else:
    raise FileNotFoundError(
        "Apple Symbols font not found in system font directories."
    )

# 1) open the compressed CSV (.bz2)
csv_path = Path(__file__).parent / "lichess_db_puzzle.csv.bz2"
stream = bz2.open(csv_path, "rt", newline="", encoding="utf-8")

# define CSV column names as Lichess publishes (no header row)
fieldnames = [
    "id","fen","moves","rating","ratingDeviation",
    "popularity","nbPlays","themes","gameUrl","openingTags"
]
reader = csv.DictReader(stream, fieldnames=fieldnames)

#
# Use reservoir sampling to select user number random puzzles near rating 1000

k = int(input("How many puzzles do you want? "))
while not k in range(1,100):
    if not k in range(1,100):
        print("Puzzle range is 1-100")
        puzzle_number=int(input("How many puzzles do you want? "))
        break

print("Reading puzzles from database...")
reservoir = []
total = 0
rating_search = int(input("What rating are you looking for? "))
tolerance = 50
while not rating_search in range(300, 3000):
    if not rating_search in range(300, 300):
        print("Please enter a rating between 300 and 3000")
        rating_search=int(input("What rating are you looking for "))
    else:
        break

# Create a progress bar for the CSV reading process
with tqdm(desc="Scanning puzzles", unit="puzzle", ncols=80, file=sys.stdout) as pbar:
    for row in reader:
        try:
            if abs(int(row["rating"]) - rating_search) <= tolerance:
                total += 1
                sol = row["moves"].split()
                item = (row["id"], row["fen"], sol)
                if total <= k:
                    reservoir.append(item)
                else:
                    r = random.randrange(total)
                    if r < k:
                        reservoir[r] = item
            pbar.update(1)
        except:
            continue

print(f"\nFound {total} puzzles matching your rating criteria")
print(f"Selected {len(reservoir)} puzzles")
print("\nGenerating PDF...")

# shuffle to randomize order on output
random.shuffle(reservoir)
selected = reservoir

#
# 3) build PDF with 9 puzzles per page
timestr = time.strftime("%d-%H:%M:%S")
pdf_filename = f"Puzzle_{timestr}.pdf"
c = canvas.Canvas(pdf_filename, pagesize=letter)
width, height = letter
per_page = 9
row_offsets = [1, 3, 5]  # row0:1in, row1:3in, row2:5in

# Wrap the loop with tqdm for progress tracking
print("Starting PDF generation...")
for idx, (pid, fen, sol) in enumerate(tqdm(selected, desc="Generating PDF", unit="puzzle", ncols=80, file=sys.stdout), start=1):
    time.sleep(0.1)  # Add a 100ms delay to see the progress bar movement
    # apply the first solution move to the FEN and update board
    board = chess.Board(fen)
    first_move = sol[0]
    board.push_san(first_move)
    fen = board.fen()
    is_black = not board.turn
    slot = (idx-1) % per_page
    if slot == 0 and idx != 1:
        c.showPage()
    square = 16  # size of each cell in points
    board_width = square * 8
    col = slot % 3
    row = slot // 3
    x_origin = inch * 0.5 + col * (board_width + inch * 0.5)
    y_origin = height - row_offsets[row] * inch * 1.2
    c.setFont("AppleSymbols", 12)
    c.drawString(x_origin, y_origin, f"Puzzle {idx}  (ID={pid})")
    y = y_origin - 8
    grid_origin_x = x_origin
    grid_origin_y = y - board_width

    # draw grid lines
    for i in range(9):
        c.line(grid_origin_x, grid_origin_y + i * square,
               grid_origin_x + 8 * square, grid_origin_y + i * square)
        c.line(grid_origin_x + i * square, grid_origin_y,
               grid_origin_x + i * square, grid_origin_y + 8 * square)

    piece_unicode = {
        'K': '♔', 'Q': '♕', 'R': '♖', 'B': '♗', 'N': '♘', 'P': '♙',
        'k': '♚', 'q': '♛', 'r': '♜', 'b': '♝', 'n': '♞', 'p': '♟'
    }

    board_ranks = fen.split()[0].split('/')
    for rank_index, rank in enumerate(board_ranks):
        file_index = 0
        for symbol in rank:
            if symbol.isdigit():
                file_index += int(symbol)
            else:
                unicode_piece = piece_unicode.get(symbol, '')
                if unicode_piece:
                    if is_black:
                        fx = 7 - file_index
                        ry = rank_index
                    else:
                        fx = file_index
                        ry = 7 - rank_index
                    cell_x = grid_origin_x + fx * square + square / 2
                    cell_y = grid_origin_y + ry * square + square / 3.5
                    c.setFont("AppleSymbols", square * 0.9)
                    c.drawCentredString(cell_x, cell_y, unicode_piece)
                file_index += 1
    side = "Black to move" if is_black else "White to move"
    c.setFont("AppleSymbols", 10)
    c.drawString(x_origin, grid_origin_y - 10, side)

    # Explicitly update and flush tqdm
    tqdm.write("", end="", file=sys.stdout)

# 4) answer page
c.showPage()
c.setFont("AppleSymbols", 12)
c.drawString(inch * 0.5, height - inch, "Solutions")
c.setFont("AppleSymbols", 10)
y = height - inch * 1.2

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
        c.setFont("AppleSymbols", 10)
        y = height - inch

c.save()