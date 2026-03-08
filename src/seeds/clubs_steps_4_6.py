"""Seed the clubs table for Steps 4–6 of the 2024-25 English pyramid.

Covers:
    Step 4  – 8 leagues  (~20 clubs each  ≈ 160 clubs)
    Step 5  – 16 leagues (~18 clubs each  ≈ 288 clubs)
    Step 6  – 19 leagues (~16 clubs each  ≈ 304 clubs)

Total: approximately 750 clubs.

Club rosters are based on the Non-League Paper pyramid poster for
2024-25.  Step 6 lists may be incomplete — gaps will be backfilled
from FA Full-Time data later.

The script is **idempotent** (PostgreSQL upsert on club name).

Usage::

    python -m src.seeds.clubs_steps_4_6
"""

import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.models import Club, League
from src.db.session import get_session

logger = logging.getLogger(__name__)

SEASON = "2024-25"

# ═══════════════════════════════════════════════════════════════════════════
# Club data — keyed by league name (must match leagues table exactly).
#
# Each value is a list of (club_name, short_name) tuples.
# website_url / pitchero_url are left None — we'll discover them via
# scraping.  The upsert won't overwrite URLs that are already populated.
# ═══════════════════════════════════════════════════════════════════════════

CLUBS: dict[str, list[tuple[str, str]]] = {

    # ╔════════════════════════════════════════════════════════════════════╗
    # ║  STEP 4 — 8 leagues                                              ║
    # ╚════════════════════════════════════════════════════════════════════╝

    # ── NPL Division One East ────────────────────────────────────────────
    "Northern Premier League Division One East": [
        ("Belper Town",                "Belper"),
        ("Brighouse Town",             "Brighouse"),
        ("Carlton Town",               "Carlton"),
        ("Cleethorpes Town",           "Cleethorpes"),
        ("Consett",                    "Consett"),
        ("Dunston UTS",                "Dunston"),
        ("Frickley Athletic",          "Frickley"),
        ("Grantham Town",              "Grantham"),
        ("Handsworth",                 "Handsworth"),
        ("Hebburn Town",               "Hebburn"),
        ("Lincoln United",             "Lincoln Utd"),
        ("Liversedge",                 "Liversedge"),
        ("Marske United",              "Marske"),
        ("Newton Aycliffe",            "Newton Aycliffe"),
        ("Ossett United",              "Ossett"),
        ("Pickering Town",             "Pickering"),
        ("Pontefract Collieries",      "Pontefract"),
        ("Shildon",                    "Shildon"),
        ("Stocksbridge Park Steels",   "Stocksbridge"),
        ("Tadcaster Albion",           "Tadcaster"),
    ],

    # ── NPL Division One Midlands ────────────────────────────────────────
    "Northern Premier League Division One Midlands": [
        ("AFC Wulfrunians",            "Wulfrunians"),
        ("Boldmere St Michaels",       "Boldmere"),
        ("Coventry Sphinx",            "Cov Sphinx"),
        ("Halesowen Town",             "Halesowen"),
        ("Heanor Town",                "Heanor"),
        ("Highgate United",            "Highgate"),
        ("Kidsgrove Athletic",         "Kidsgrove"),
        ("Long Eaton United",          "Long Eaton"),
        ("Loughborough Dynamo",        "Loughborough"),
        ("Market Drayton Town",        "Market Drayton"),
        ("Newcastle Town",             "Newcastle T"),
        ("Quorn",                      "Quorn"),
        ("Rocester",                   "Rocester"),
        ("Rugby Town",                 "Rugby"),
        ("Shepshed Dynamo",            "Shepshed"),
        ("Sporting Khalsa",            "Khalsa"),
        ("Sutton Coldfield Town",      "Sutton C"),
        ("Walsall Wood",               "Walsall Wood"),
        ("Worksop Town",               "Worksop"),
        ("Uttoxeter Town",             "Uttoxeter"),
    ],

    # ── NPL Division One West ───────────────────────────────────────────
    "Northern Premier League Division One West": [
        ("Avro",                       "Avro"),
        ("Bootle",                     "Bootle"),
        ("Burscough",                  "Burscough"),
        ("City of Liverpool",          "C of Liverpool"),
        ("Clitheroe",                  "Clitheroe"),
        ("Colne",                      "Colne"),
        ("Congleton Town",             "Congleton"),
        ("Droylsden",                  "Droylsden"),
        ("Hanley Town",                "Hanley"),
        ("Irlam",                      "Irlam"),
        ("Kendal Town",                "Kendal"),
        ("Litherland REMYCA",          "Litherland"),
        ("Longridge Town",             "Longridge"),
        ("Mossley",                    "Mossley"),
        ("Padiham",                    "Padiham"),
        ("Prescot Cables",             "Prescot"),
        ("Ramsbottom United",          "Ramsbottom"),
        ("Runcorn Linnets",            "Runcorn"),
        ("Squires Gate",               "Squires Gate"),
        ("Trafford",                   "Trafford"),
    ],

    # ── Southern League Division One Central ─────────────────────────────
    "Southern League Division One Central": [
        ("AFC Dunstable",              "Dunstable"),
        ("Aylesbury United",           "Aylesbury"),
        ("Bedford Town",               "Bedford"),
        ("Berkhamsted",                "Berkhamsted"),
        ("Bury Town",                  "Bury Town"),
        ("Coleshill Town",             "Coleshill"),
        ("Daventry Town",              "Daventry"),
        ("Didcot Town",                "Didcot"),
        ("Dunstable Town",             "Dunstable T"),
        ("Easington Sports",           "Easington"),
        ("Eynesbury Rovers",           "Eynesbury"),
        ("Harborough Town",            "Harborough"),
        ("Milton Keynes Irish",        "MK Irish"),
        ("Newport Pagnell Town",       "Newport Pagnell"),
        ("North Leigh",                "North Leigh"),
        ("Potton United",              "Potton"),
        ("Thame United",               "Thame"),
        ("Wellingborough Town",        "Wellingborough"),
        ("Wisbech Town",               "Wisbech"),
        ("Yaxley",                     "Yaxley"),
    ],

    # ── Southern League Division One South ───────────────────────────────
    "Southern League Division One South": [
        ("AFC Stoneham",               "Stoneham"),
        ("Bashley",                    "Bashley"),
        ("Bemerton Heath Harlequins",  "Bemerton"),
        ("Bideford",                   "Bideford"),
        ("Bristol Manor Farm",         "Bristol MF"),
        ("Cinderford Town",            "Cinderford"),
        ("Evesham United",             "Evesham"),
        ("Exmouth Town",               "Exmouth"),
        ("Falmouth Town",              "Falmouth"),
        ("Hamworthy United",           "Hamworthy"),
        ("Larkhall Athletic",          "Larkhall"),
        ("Mangotsfield United",        "Mangotsfield"),
        ("Melksham Town",              "Melksham"),
        ("Paulton Rovers",             "Paulton"),
        ("Sherborne Town",             "Sherborne"),
        ("Sholing",                    "Sholing"),
        ("Street",                     "Street"),
        ("Westbury United",            "Westbury"),
        ("Willand Rovers",             "Willand"),
        ("Winchester City",            "Winchester"),
    ],

    # ── Isthmian League Division One North ───────────────────────────────
    "Isthmian League Division One North": [
        ("AFC Sudbury",                "Sudbury"),
        ("Barking",                    "Barking"),
        ("Basildon United",            "Basildon"),
        ("Brentwood Town",             "Brentwood"),
        ("Coggeshall Town",            "Coggeshall"),
        ("Dereham Town",               "Dereham"),
        ("Felixstowe & Walton United", "Felixstowe"),
        ("Gorleston",                  "Gorleston"),
        ("Grays Athletic",             "Grays"),
        ("Great Wakering Rovers",      "Gt Wakering"),
        ("Hashtag United",             "Hashtag"),
        ("Heybridge Swifts",           "Heybridge"),
        ("Hullbridge Sports",          "Hullbridge"),
        ("Maldon & Tiptree",           "Maldon"),
        ("Romford",                    "Romford"),
        ("Soham Town Rangers",         "Soham"),
        ("Stowmarket Town",            "Stowmarket"),
        ("Tilbury",                    "Tilbury"),
        ("Witham Town",                "Witham"),
        ("Wroxham",                    "Wroxham"),
    ],

    # ── Isthmian League Division One South Central ───────────────────────
    "Isthmian League Division One South Central": [
        ("Beckenham Town",             "Beckenham"),
        ("Bedfont Sports",             "Bedfont"),
        ("Burgess Hill Town",          "Burgess Hill"),
        ("Chipstead",                  "Chipstead"),
        ("Croydon",                    "Croydon"),
        ("Eastbourne Town",            "Eastbourne T"),
        ("Faversham Town",             "Faversham"),
        ("Fleet Town",                 "Fleet Town"),
        ("Guernsey",                   "Guernsey"),
        ("Hanwell Town",               "Hanwell"),
        ("Haywards Heath Town",        "Haywards H"),
        ("Lancing",                    "Lancing"),
        ("Littlehampton Town",         "Littlehampton"),
        ("Merstham",                   "Merstham"),
        ("Sevenoaks Town",             "Sevenoaks"),
        ("South Park",                 "South Park"),
        ("Spelthorne Sports",          "Spelthorne"),
        ("Tooting & Mitcham United",   "Tooting"),
        ("Ware",                       "Ware"),
        ("Whyteleafe",                 "Whyteleafe"),
    ],

    # ── Isthmian League Division One South East ─────────────────────────
    "Isthmian League Division One South East": [
        ("Ashford United",             "Ashford"),
        ("Crowborough Athletic",       "Crowborough"),
        ("Deal Town",                  "Deal"),
        ("East Grinstead Town",        "East Grinstead"),
        ("Erith Town",                 "Erith"),
        ("Erith & Belvedere",          "Erith & Belv"),
        ("Fisher",                     "Fisher"),
        ("Herne Bay",                  "Herne Bay"),
        ("Hythe Town",                 "Hythe"),
        ("Lydd Town",                  "Lydd"),
        ("Lordswood",                  "Lordswood"),
        ("Phoenix Sports",             "Phoenix"),
        ("Ramsgate",                   "Ramsgate"),
        ("Sheppey United",             "Sheppey"),
        ("Sittingbourne",              "Sittingbourne"),
        ("Three Bridges",              "Three Bridges"),
        ("Tunbridge Wells",            "Tunbridge W"),
        ("VCD Athletic",               "VCD"),
        ("Whitstable Town",            "Whitstable"),
        ("Meridian VP",                "Meridian"),
    ],

    # ╔════════════════════════════════════════════════════════════════════╗
    # ║  STEP 5 — 16 leagues                                             ║
    # ╚════════════════════════════════════════════════════════════════════╝

    # ── Combined Counties League Premier Division North ──────────────────
    "Combined Counties League Premier Division North": [
        ("Ascot United",               "Ascot"),
        ("Badshot Lea",                "Badshot Lea"),
        ("Balham",                     "Balham"),
        ("Bedfont & Feltham",          "Bedfont & F"),
        ("Camberley Town",             "Camberley"),
        ("CB Hounslow United",         "CB Hounslow"),
        ("Cobham",                     "Cobham"),
        ("Colliers Wood United",       "Colliers Wood"),
        ("Egham Town",                 "Egham"),
        ("Frimley Green",              "Frimley"),
        ("Hanworth Villa",             "Hanworth"),
        ("Knaphill",                   "Knaphill"),
        ("Raynes Park Vale",           "Raynes Park"),
        ("Sandhurst Town",             "Sandhurst"),
        ("Sheerwater",                 "Sheerwater"),
        ("Virginia Water",             "Virginia W"),
        ("Westside",                   "Westside"),
        ("Fleet Spurs",                "Fleet Spurs"),
    ],

    # ── Combined Counties League Premier Division South ──────────────────
    "Combined Counties League Premier Division South": [
        ("Abbey Rangers",              "Abbey Rangers"),
        ("Banstead Athletic",          "Banstead"),
        ("Bookham",                    "Bookham"),
        ("Chessington & Hook United",  "Chessington"),
        ("Epsom & Ewell",              "Epsom"),
        ("Farnham Town",               "Farnham"),
        ("Godalming Town",             "Godalming"),
        ("Guildford City",             "Guildford C"),
        ("Horley Town",                "Horley"),
        ("Kensington Borough",         "Kensington"),
        ("Mole Valley SCR",            "Mole Valley"),
        ("Redhill",                    "Redhill"),
        ("Southall",                   "Southall"),
        ("Westfield",                  "Westfield"),
        ("Worcester Park",             "Worcester Pk"),
        ("Ash United",                 "Ash Utd"),
        ("Cove",                       "Cove"),
        ("Bagshot",                    "Bagshot"),
    ],

    # ── Eastern Counties Football League Premier Division ────────────────
    "Eastern Counties Football League Premier Division": [
        ("Brantham Athletic",          "Brantham"),
        ("Downham Town",               "Downham"),
        ("Ely City",                   "Ely"),
        ("Framlingham Town",           "Framlingham"),
        ("Godmanchester Rovers",       "Godmanchester"),
        ("Great Yarmouth Town",        "Gt Yarmouth"),
        ("Hadleigh United",            "Hadleigh"),
        ("Haverhill Rovers",           "Haverhill"),
        ("Kirkley & Pakefield",        "Kirkley"),
        ("Long Melford",               "Long Melford"),
        ("March Town United",          "March Town"),
        ("Mildenhall Town",            "Mildenhall"),
        ("Newmarket Town",             "Newmarket"),
        ("Swaffham Town",              "Swaffham"),
        ("Thetford Town",              "Thetford"),
        ("Woodbridge Town",            "Woodbridge"),
        ("Walsham-le-Willows",         "Walsham"),
        ("Stanway Rovers",             "Stanway"),
    ],

    # ── Essex Senior Football League ────────────────────────────────────
    "Essex Senior Football League": [
        ("Barking & Dagenham",         "Barking & D"),
        ("Burnham Ramblers",           "Burnham"),
        ("Catholic United",            "Catholic Utd"),
        ("Clapton",                    "Clapton"),
        ("Enfield Borough",            "Enfield B"),
        ("FC Romania",                 "FC Romania"),
        ("Hackney Wick",               "Hackney Wick"),
        ("Hadley",                     "Hadley"),
        ("Harold Wood Athletic",       "Harold Wood"),
        ("Ilford",                     "Ilford"),
        ("Redbridge",                  "Redbridge"),
        ("Saffron Walden Town",        "Saffron W"),
        ("Sawbridgeworth Town",        "Sawbridge"),
        ("Southend Manor",             "Southend M"),
        ("Sporting Bengal United",     "Sporting Bengal"),
        ("Stansted",                   "Stansted"),
        ("Tower Hamlets",              "Tower Hamlets"),
        ("Walthamstow",                "Walthamstow"),
    ],

    # ── Hellenic Football League Premier Division ───────────────────────
    "Hellenic Football League Premier Division": [
        ("Abingdon United",            "Abingdon"),
        ("Ardley United",              "Ardley"),
        ("Binfield",                   "Binfield"),
        ("Bracknell Town",             "Bracknell"),
        ("Burnham",                    "Burnham"),
        ("Carterton",                  "Carterton"),
        ("Flackwell Heath",            "Flackwell"),
        ("Highmoor-IBIS",              "Highmoor"),
        ("Holmer Green",               "Holmer Green"),
        ("Kidlington",                 "Kidlington"),
        ("Lydney Town",                "Lydney"),
        ("Marlow United",              "Marlow Utd"),
        ("Reading City",               "Reading C"),
        ("Shrivenham",                 "Shrivenham"),
        ("Tuffley Rovers",             "Tuffley"),
        ("Shortwood United",           "Shortwood"),
        ("Wantage Town",               "Wantage"),
        ("Woodley United",             "Woodley"),
    ],

    # ── Midland Football League Premier Division ─────────────────────────
    "Midland Football League Premier Division": [
        ("Atherstone Town",            "Atherstone"),
        ("Brocton",                    "Brocton"),
        ("Cadbury Athletic",           "Cadbury"),
        ("Coventry United",            "Coventry Utd"),
        ("Darlaston Town 1874",        "Darlaston"),
        ("Dudley Town",                "Dudley"),
        ("GNP Sports",                 "GNP"),
        ("Heath Hayes",                "Heath Hayes"),
        ("Lye Town",                   "Lye Town"),
        ("Nuneaton Griff",             "Nuneaton G"),
        ("Paget Rangers",              "Paget"),
        ("Racing Club Warwick",        "RC Warwick"),
        ("Studley",                    "Studley"),
        ("Tividale",                   "Tividale"),
        ("Uttoxeter Town FC",          "Uttoxeter FC"),
        ("Wednesfield",                "Wednesfield"),
        ("Wolverhampton Casuals",      "Wolves Casuals"),
        ("Worcester Raiders",          "Worcester R"),
    ],

    # ── North West Counties Football League Premier Division ─────────────
    "North West Counties Football League Premier Division": [
        ("1874 Northwich",             "1874 Northwich"),
        ("Abbey Hulton United",        "Abbey Hulton"),
        ("Barnoldswick Town",          "Barnoldswick"),
        ("Bury AFC",                   "Bury AFC"),
        ("Charnock Richard",           "Charnock R"),
        ("Cheadle Heath Nomads",       "Cheadle H"),
        ("Eccleshall FC",              "Eccleshall FC"),
        ("Golcar United",              "Golcar"),
        ("Lower Breck",                "Lower Breck"),
        ("Macclesfield Town",          "Macclesfield T"),
        ("Maine Road",                 "Maine Road"),
        ("Northwich Victoria",         "Northwich V"),
        ("Pilkington",                 "Pilkington"),
        ("Skelmersdale United",        "Skelmersdale"),
        ("St Helens Town",             "St Helens"),
        ("Vauxhall Motors",            "Vauxhall"),
        ("West Didsbury & Chorlton",   "West Dids"),
        ("Winsford United",            "Winsford"),
    ],

    # ── Northern Football League Division One ────────────────────────────
    "Northern Football League Division One": [
        ("Billingham Synthonia",       "Billingham S"),
        ("Billingham Town",            "Billingham T"),
        ("Bishop Auckland",            "Bishop Auckland"),
        ("Crook Town",                 "Crook"),
        ("Durham City",                "Durham"),
        ("Guisborough Town",           "Guisborough"),
        ("Heaton Stannington",         "Heaton Stan"),
        ("Jarrow",                     "Jarrow"),
        ("Northallerton Town",         "Northallerton"),
        ("North Shields",              "North Shields"),
        ("Penrith",                    "Penrith"),
        ("Redcar Athletic",            "Redcar"),
        ("Seaham Red Star",            "Seaham"),
        ("Sunderland RCA",             "Sunderland RCA"),
        ("Thornaby",                   "Thornaby"),
        ("Tow Law Town",              "Tow Law"),
        ("Washington",                 "Washington"),
        ("West Auckland Town",         "West Auckland"),
    ],

    # ── Northern Counties East Football League Premier Division ──────────
    "Northern Counties East Football League Premier Division": [
        ("Armthorpe Welfare",          "Armthorpe"),
        ("Bottesford Town",            "Bottesford"),
        ("Clipstone",                  "Clipstone"),
        ("Eccleshill United",          "Eccleshill"),
        ("Emley AFC",                  "Emley"),
        ("Garforth Town",              "Garforth"),
        ("Glasshoughton Welfare",      "Glasshoughton"),
        ("Hall Road Rangers",          "Hall Road"),
        ("Hallam",                     "Hallam"),
        ("Hemsworth Miners Welfare",   "Hemsworth"),
        ("Maltby Main",                "Maltby"),
        ("Nostell Miners Welfare",     "Nostell MW"),
        ("Penistone Church",           "Penistone"),
        ("Retford United",             "Retford"),
        ("Rossington Main",            "Rossington"),
        ("Selby Town",                 "Selby"),
        ("Swallownest",                "Swallownest"),
        ("Winterton Rangers",          "Winterton"),
    ],

    # ── Southern Combination Football League Premier Division ────────────
    "Southern Combination Football League Premier Division": [
        ("AFC Uckfield Town",          "Uckfield"),
        ("Alfold",                     "Alfold"),
        ("Broadbridge Heath",          "Broadbridge"),
        ("Crawley Down Gatwick",       "Crawley Down"),
        ("Eastbourne United",          "Eastbourne U"),
        ("East Preston",               "East Preston"),
        ("Hassocks",                   "Hassocks"),
        ("Lingfield",                  "Lingfield"),
        ("Midhurst & Easebourne",      "Midhurst"),
        ("Newhaven",                   "Newhaven"),
        ("Pagham",                     "Pagham"),
        ("Peacehaven & Telscombe",     "Peacehaven"),
        ("Roffey",                     "Roffey"),
        ("Saltdean United",            "Saltdean"),
        ("Selsey",                     "Selsey"),
        ("Shoreham",                   "Shoreham"),
        ("Steyning Town Community",    "Steyning"),
        ("Upper Beeding",              "Upper Beeding"),
    ],

    # ── Southern Counties East Football League Premier Division ──────────
    "Southern Counties East Football League Premier Division": [
        ("Bearsted",                   "Bearsted"),
        ("Bromley Green",              "Bromley Grn"),
        ("Bridon Ropes",               "Bridon"),
        ("Canterbury City",            "Canterbury"),
        ("Corinthian",                 "Corinthian"),
        ("Croydon Athletic",           "Croydon Ath"),
        ("Glebe",                      "Glebe"),
        ("Greenways",                  "Greenways"),
        ("Guru Nanak",                 "Guru Nanak"),
        ("Hollands & Blair",           "Hollands"),
        ("Kennington",                 "Kennington"),
        ("Lewisham Borough",           "Lewisham B"),
        ("Larkfield & New Hythe",      "Larkfield"),
        ("Punjab United",              "Punjab"),
        ("Rusthall",                   "Rusthall"),
        ("Snodland Town",              "Snodland"),
        ("Tower Hamlets FC",           "Tower H FC"),
        ("Welling Town",               "Welling Town"),
    ],

    # ── Spartan South Midlands Football League Premier Division ──────────
    "Spartan South Midlands Football League Premier Division": [
        ("Arlesey Town",               "Arlesey"),
        ("Baldock Town",               "Baldock"),
        ("Broadfields United",         "Broadfields"),
        ("Cockfosters",                "Cockfosters"),
        ("Colney Heath",               "Colney Heath"),
        ("Crawley Green",              "Crawley Green"),
        ("Edgware Town",               "Edgware"),
        ("Hadley Wood & Wingate",      "Hadley Wood"),
        ("Harpenden Town",             "Harpenden"),
        ("Langford",                   "Langford"),
        ("Leverstock Green",           "Leverstock"),
        ("London Colney",              "London Colney"),
        ("Oxhey Jets",                 "Oxhey"),
        ("Risborough Rangers",         "Risborough"),
        ("Tring Athletic",             "Tring"),
        ("Wembley",                    "Wembley"),
        ("Welwyn Garden City",         "Welwyn GC"),
        ("New Salamis",                "New Salamis"),
    ],

    # ── United Counties League Premier Division North ────────────────────
    "United Counties League Premier Division North": [
        ("Anstey Nomads",              "Anstey"),
        ("Bourne Town",                "Bourne"),
        ("Deeping Rangers",            "Deeping"),
        ("Harrowby United",            "Harrowby"),
        ("Holbeach United",            "Holbeach"),
        ("Huntingdon Town",            "Huntingdon"),
        ("Kirby Muxloe",              "Kirby Muxloe"),
        ("Leicester Nirvana",          "Leicester N"),
        ("Long Buckby",                "Long Buckby"),
        ("Lutterworth Athletic",       "Lutterworth"),
        ("Melton Town",                "Melton"),
        ("Pinchbeck United",           "Pinchbeck"),
        ("Rothwell Corinthians",       "Rothwell"),
        ("Sileby Rangers",             "Sileby"),
        ("Sleaford Town",              "Sleaford"),
        ("Spalding United",            "Spalding"),
        ("Stamford Belvedere",         "Stamford B"),
        ("Whitworth",                  "Whitworth"),
    ],

    # ── United Counties League Premier Division South ────────────────────
    "United Counties League Premier Division South": [
        ("AFC Kempston Rovers",        "Kempston"),
        ("Brackley Town Saints",       "Brackley Sts"),
        ("Buckingham Athletic",        "Buckingham"),
        ("Bugbrooke St Michaels",      "Bugbrooke"),
        ("Burton Park Wanderers",      "Burton Park"),
        ("Cogenhoe United",            "Cogenhoe"),
        ("Desborough Town",            "Desborough"),
        ("Irchester United",           "Irchester"),
        ("Northampton ON Chenecks",    "ON Chenecks"),
        ("Northampton Sileby Rangers", "Nth Sileby"),
        ("Olney Town",                 "Olney"),
        ("Raunds Town",                "Raunds"),
        ("Rothwell Town",              "Rothwell T"),
        ("Rushden & Higham United",    "Rushden & H"),
        ("St Neots Town Saints",       "St Neots Sts"),
        ("Towcester Town",             "Towcester"),
        ("Wellingborough Whitworth",   "W'boro W"),
        ("Woodford United",            "Woodford"),
    ],

    # ── Wessex Football League Premier Division ─────────────────────────
    "Wessex Football League Premier Division": [
        ("Alresford Town",             "Alresford"),
        ("Alton",                      "Alton"),
        ("Amesbury Town",              "Amesbury"),
        ("Andover New Street",         "Andover NS"),
        ("Baffins Milton Rovers",      "Baffins"),
        ("Blackfield & Langley",       "Blackfield"),
        ("Bournemouth",                "Bournemouth FC"),
        ("Brockenhurst",               "Brockenhurst"),
        ("Christchurch",               "Christchurch"),
        ("Cowes Sports",               "Cowes"),
        ("Fareham Town",               "Fareham"),
        ("Hamble Club",                "Hamble"),
        ("Horndean",                   "Horndean"),
        ("Lymington Town",             "Lymington"),
        ("Moneyfields",                "Moneyfields"),
        ("Petersfield Town",           "Petersfield"),
        ("Portland United",            "Portland"),
        ("United Services Portsmouth", "US Portsmouth"),
    ],

    # ── Western Football League Premier Division ────────────────────────
    "Western Football League Premier Division": [
        ("Bishop Sutton",              "Bishop Sutton"),
        ("Bitton",                     "Bitton"),
        ("Bradford Town",              "Bradford T"),
        ("Bridgwater United",          "Bridgwater"),
        ("Brislington",                "Brislington"),
        ("Cadbury Heath",              "Cadbury H"),
        ("Clevedon Town",              "Clevedon"),
        ("Corsham Town",               "Corsham"),
        ("Hengrove Athletic",          "Hengrove"),
        ("Ilfracombe Town",            "Ilfracombe"),
        ("Keynsham Town",              "Keynsham"),
        ("Odd Down",                   "Odd Down"),
        ("Shepton Mallet",             "Shepton M"),
        ("Tavistock",                  "Tavistock"),
        ("Wellington",                 "Wellington"),
        ("Wells City",                 "Wells"),
        ("Westbury United FC",         "Westbury FC"),
        ("Wincanton Town",             "Wincanton"),
    ],

    # ╔════════════════════════════════════════════════════════════════════╗
    # ║  STEP 6 — 19 leagues                                             ║
    # ║  Lists are partial — will be backfilled from FA Full-Time.        ║
    # ╚════════════════════════════════════════════════════════════════════╝

    # ── Combined Counties League Division One ───────────────────────────
    "Combined Counties League Division One": [
        ("AFC Aldermaston",            "Aldermaston"),
        ("British Airways",            "BA"),
        ("Cranleigh",                  "Cranleigh"),
        ("Deportivo Galicia",          "Deportivo G"),
        ("Eversley & California",      "Eversley"),
        ("Feltham",                    "Feltham"),
        ("Hartley Wintney Reserves",   "Hartley W Res"),
        ("Kew Association",            "Kew"),
        ("Molesey",                    "Molesey"),
        ("NPL",                        "NPL FC"),
        ("Ottershaw",                  "Ottershaw"),
        ("Parkgate",                   "Parkgate FC"),
        ("Rayners Lane",              "Rayners Lane"),
        ("Shaftesbury",                "Shaftesbury"),
        ("Staines Lammas",             "Staines L"),
        ("Walton & Hersham Reserves",  "Walton Res"),
    ],

    # ── Eastern Counties Football League Division One North ──────────────
    "Eastern Counties Football League Division One North": [
        ("Acle United",                "Acle"),
        ("Beccles Town",               "Beccles"),
        ("Brandon Town",               "Brandon"),
        ("Diss Town",                  "Diss"),
        ("Fakenham Town",              "Fakenham"),
        ("Great Yarmouth Town Res",    "Gt Yarmouth R"),
        ("Harleston Town",             "Harleston"),
        ("Hemsby",                     "Hemsby"),
        ("King's Lynn Town Reserves",  "King's Lynn R"),
        ("Mattishall",                 "Mattishall"),
        ("Mulbarton Wanderers",        "Mulbarton"),
        ("Norwich CBS",                "Norwich CBS"),
        ("Sheringham",                 "Sheringham"),
        ("Stalham Town",               "Stalham"),
        ("Thetford Rovers",            "Thetford R"),
        ("Wells Town",                 "Wells Town"),
    ],

    # ── Eastern Counties Football League Division One South ──────────────
    "Eastern Counties Football League Division One South": [
        ("Brightlingsea Regent",       "Brightlingsea"),
        ("Capel Plough",               "Capel Plough"),
        ("Cornard United",             "Cornard"),
        ("FC Clacton",                 "FC Clacton"),
        ("Halstead Town",              "Halstead"),
        ("Harwich & Parkeston",        "Harwich"),
        ("Holland",                    "Holland FC"),
        ("Ipswich Wanderers",          "Ipswich W"),
        ("Kelvedon Hatch",             "Kelvedon"),
        ("Lawford Lads",               "Lawford"),
        ("Little Oakley",              "Little Oakley"),
        ("Needham Market Reserves",    "Needham Res"),
        ("Takeley",                    "Takeley"),
        ("Tiptree Heath",              "Tiptree H"),
        ("Wivenhoe Town",              "Wivenhoe"),
        ("Woodford Town",              "Woodford T"),
    ],

    # ── Hellenic Football League Division One ───────────────────────────
    "Hellenic Football League Division One": [
        ("Abingdon Town",              "Abingdon T"),
        ("Cheltenham Saracens",        "Cheltenham S"),
        ("Clanfield 85",               "Clanfield"),
        ("Cricklade Town",             "Cricklade"),
        ("Easington Sports FC",        "Easington FC"),
        ("Fairford Town",              "Fairford"),
        ("Headington Amateurs",        "Headington"),
        ("Hook Norton",                "Hook Norton"),
        ("Malmesbury Victoria",        "Malmesbury"),
        ("Middle Barton",              "Middle Barton"),
        ("Milton United",              "Milton Utd"),
        ("New College Swindon",        "New College"),
        ("Old Woodstock Town",         "Old Woodstock"),
        ("Pewsey Vale",                "Pewsey"),
        ("Purton",                     "Purton"),
        ("Tytherington Rocks",         "Tytherington"),
    ],

    # ── Hellenic Football League Division Two ───────────────────────────
    "Hellenic Football League Division Two": [
        ("Adderbury Park",             "Adderbury"),
        ("Bampton Town",               "Bampton"),
        ("Bletchingdon",               "Bletchingdon"),
        ("Bourton Rovers",             "Bourton"),
        ("Chalgrove Cavaliers",        "Chalgrove"),
        ("Chipping Norton Town",       "Chipping Norton"),
        ("Cirencester Town Development","Cirencester D"),
        ("Faringdon Town",             "Faringdon"),
        ("Freeland",                   "Freeland"),
        ("Kirtlington",                "Kirtlington"),
        ("Long Crendon",               "Long Crendon"),
        ("North Oxford",               "North Oxford"),
        ("Stonesfield",                "Stonesfield"),
        ("Summertown Stars",           "Summertown"),
        ("Watlington Town",            "Watlington"),
        ("Witney Royals",              "Witney R"),
    ],

    # ── Midland Football League Division One ────────────────────────────
    "Midland Football League Division One": [
        ("Alcester Town",              "Alcester"),
        ("Barnt Green Spartak",        "Barnt Green"),
        ("Bilston Town Community",     "Bilston"),
        ("Castle Vale Town",           "Castle Vale"),
        ("Coton Green",                "Coton Green"),
        ("Earlswood Town",             "Earlswood"),
        ("FC Stratford",               "FC Stratford"),
        ("Feckenham",                  "Feckenham"),
        ("Henley Forest",              "Henley Forest"),
        ("Knowle",                     "Knowle"),
        ("Lane Head",                  "Lane Head"),
        ("Littleton",                  "Littleton"),
        ("Montpellier",                "Montpellier"),
        ("Northfield Town",            "Northfield"),
        ("Pershore Town",              "Pershore"),
        ("Shipston Excelsior",         "Shipston"),
    ],

    # ── Midland Football League Division Two ────────────────────────────
    "Midland Football League Division Two": [
        ("Bartley Green",              "Bartley Grn"),
        ("Bloxwich Town",              "Bloxwich"),
        ("Bolehall Swifts",            "Bolehall"),
        ("Continental Star",           "Continental"),
        ("Coventry Alvis",             "Cov Alvis"),
        ("Droitwich Spa",              "Droitwich"),
        ("Hampton",                    "Hampton MFL"),
        ("Kenilworth Wardens",         "Kenilworth"),
        ("Leamington Hibernian",       "Leamington H"),
        ("Moors Academy",              "Moors Acad"),
        ("Polesworth",                 "Polesworth"),
        ("Rugby Borough",              "Rugby Boro"),
        ("Smithswood Firs",            "Smithswood"),
        ("Southam United",             "Southam"),
        ("Tipton Town",                "Tipton"),
        ("Wyrley Rangers",             "Wyrley"),
    ],

    # ── North West Counties Football League Division One North ──────────
    "North West Counties Football League Division One North": [
        ("AFC Blackpool",              "AFC Blackpool"),
        ("AFC Liverpool",              "AFC Liverpool"),
        ("Bacup Borough",              "Bacup"),
        ("Campion",                    "Campion"),
        ("Cleator Moor Celtic",        "Cleator Moor"),
        ("Daisy Hill",                 "Daisy Hill"),
        ("Euxton Villa",               "Euxton"),
        ("Garstang",                   "Garstang"),
        ("Holker Old Boys",            "Holker"),
        ("Nelson",                     "Nelson"),
        ("Shelley",                    "Shelley"),
        ("Steeton",                    "Steeton"),
        ("Thornton Cleveleys",         "Thornton C"),
        ("Vics",                       "Vics"),
        ("Whitehaven",                 "Whitehaven"),
        ("Wythenshawe Amateurs",       "Wythenshawe"),
    ],

    # ── North West Counties Football League Division One South ──────────
    "North West Counties Football League Division One South": [
        ("Alsager Town",               "Alsager"),
        ("Barnton",                    "Barnton"),
        ("Brereton Social",            "Brereton"),
        ("Cheadle Town",               "Cheadle T"),
        ("Eccleshall",                 "Eccleshall"),
        ("Ellesmere Port Town",        "Ellesmere P"),
        ("Greenalls Padgate St Oswalds","Greenalls"),
        ("Middlewich Town",            "Middlewich"),
        ("New Mills",                  "New Mills"),
        ("Rylands",                    "Rylands FC"),
        ("Sandbach United",            "Sandbach"),
        ("Stafford Town",              "Stafford T"),
        ("Stone Old Alleynians",       "Stone OA"),
        ("Whaley Bridge",              "Whaley Bridge"),
        ("Whitchurch Alport",          "Whitchurch"),
        ("Wythenshawe Town",           "Wythenshawe T"),
    ],

    # ── Northern Football League Division Two ───────────────────────────
    "Northern Football League Division Two": [
        ("Alnwick Town",               "Alnwick"),
        ("Ashington",                  "Ashington"),
        ("Bedlington Terriers",        "Bedlington"),
        ("Birtley Town",               "Birtley"),
        ("Brandon United",             "Brandon Utd"),
        ("Chester-le-Street United",   "Chester-le-St"),
        ("Esh Winning",                "Esh Winning"),
        ("Gateshead Leam Rangers",     "Gateshead LR"),
        ("Jarrow FC",                  "Jarrow FC"),
        ("Prudhoe YC",                 "Prudhoe"),
        ("Ryhope CW",                  "Ryhope"),
        ("Sunderland West End",        "Sunderland WE"),
        ("Team Northumbria",           "Team Northumb"),
        ("Whitley Bay",                "Whitley Bay"),
        ("Willington",                 "Willington"),
        ("Windscale",                  "Windscale"),
    ],

    # ── Northern Counties East Football League Division One ──────────────
    "Northern Counties East Football League Division One": [
        ("AFC Emley",                  "AFC Emley"),
        ("Appleby Frodingham",         "Appleby F"),
        ("Askern",                     "Askern"),
        ("Brigg Town",                 "Brigg"),
        ("Campion AFC",                "Campion AFC"),
        ("Clay Cross Town",            "Clay Cross"),
        ("Dinnington Town",            "Dinnington"),
        ("Glasshoughton Welfare FC",   "Glasshoughton FC"),
        ("Grimsby Borough",            "Grimsby B"),
        ("Handsworth Parramore",       "Handsworth P"),
        ("Harworth Colliery",          "Harworth"),
        ("Knaresborough Town",         "Knaresborough"),
        ("Rainworth Miners Welfare",   "Rainworth"),
        ("Staveley Miners Welfare",    "Staveley"),
        ("Thackley",                   "Thackley"),
        ("Worsbrough Bridge Athletic", "Worsbrough"),
    ],

    # ── Southern Combination Football League Division One ────────────────
    "Southern Combination Football League Division One": [
        ("AFC Varndeanians",           "Varndeanians"),
        ("Bexhill United",             "Bexhill"),
        ("Billingshurst",              "Billingshurst"),
        ("Bosham",                     "Bosham"),
        ("Copthorne",                  "Copthorne"),
        ("Dorking Wanderers Reserves", "Dorking Res"),
        ("Epsom & Ewell FC",           "Epsom FC"),
        ("Ferring",                    "Ferring"),
        ("Hailsham Town",              "Hailsham"),
        ("Hurstpierpoint",             "Hurst"),
        ("Jarvis Brook",               "Jarvis Brook"),
        ("Mile Oak",                   "Mile Oak"),
        ("Oakwood",                    "Oakwood"),
        ("Ringmer",                    "Ringmer"),
        ("Sidlesham",                  "Sidlesham"),
        ("Storrington",                "Storrington"),
    ],

    # ── Southern Combination Football League Division Two ────────────────
    "Southern Combination Football League Division Two": [
        ("Angmering",                  "Angmering"),
        ("Arundel",                    "Arundel"),
        ("Bognor Regis Town Reserves", "Bognor Res"),
        ("Brighton Electricity",       "Brighton E"),
        ("Clymping",                   "Clymping"),
        ("Cowfold",                    "Cowfold"),
        ("Franklands Village",         "Franklands"),
        ("Henfield",                   "Henfield"),
        ("Ifield",                     "Ifield"),
        ("Lewes Reserves",             "Lewes Res"),
        ("Littlehampton United",       "Littlehampton U"),
        ("Rottingdean Village",        "Rottingdean"),
        ("Rustington",                 "Rustington"),
        ("Seaford Town",               "Seaford"),
        ("Southwick",                  "Southwick"),
        ("Steyning Town Reserves",     "Steyning Res"),
    ],

    # ── Southern Counties East Football League Division One ──────────────
    "Southern Counties East Football League Division One": [
        ("AFC Croydon Athletic",       "Croydon A"),
        ("Cray Valley PM",             "Cray Valley"),
        ("Deal Town FC",               "Deal T FC"),
        ("Forest Hill Park",           "Forest Hill"),
        ("Greenwich Borough",          "Greenwich"),
        ("Ide Hill",                   "Ide Hill"),
        ("Kent Football United",       "Kent FU"),
        ("Lewisham Borough Community", "Lewisham BC"),
        ("Meridian VP FC",             "Meridian FC"),
        ("Metrogas",                   "Metrogas"),
        ("New Romney",                 "New Romney"),
        ("Otford United",              "Otford"),
        ("Peckham Town",               "Peckham"),
        ("Shoreham FC",                "Shoreham FC"),
        ("Stansfeld",                  "Stansfeld"),
        ("Wateringbury",               "Wateringbury"),
    ],

    # ── Spartan South Midlands Football League Division One ──────────────
    "Spartan South Midlands Football League Division One": [
        ("Amersham Town",              "Amersham"),
        ("Ampthill Town",              "Ampthill"),
        ("Bovingdon",                  "Bovingdon"),
        ("Brimsdown",                  "Brimsdown"),
        ("Buckingham Town",            "Buckingham T"),
        ("Codicote",                   "Codicote"),
        ("Dunton & Broughton Rangers", "Dunton"),
        ("Eaton Bray United",          "Eaton Bray"),
        ("Harefield United",           "Harefield"),
        ("Hillingdon Borough",         "Hillingdon"),
        ("Holmer Green FC",            "Holmer Grn FC"),
        ("Kings Langley Reserves",     "Kings L Res"),
        ("London Lions",               "London Lions"),
        ("Stotfold",                   "Stotfold"),
        ("Sun Sports",                 "Sun Sports"),
        ("Wodson Park",                "Wodson Park"),
    ],

    # ── Spartan South Midlands Football League Division Two ──────────────
    "Spartan South Midlands Football League Division Two": [
        ("Aston Clinton",              "Aston Clinton"),
        ("Berkhamsted Raiders",        "Berkh Raiders"),
        ("Caddington",                 "Caddington"),
        ("De Havilland",               "De Havilland"),
        ("Harpenden Town Reserves",    "Harpenden Res"),
        ("Hatfield Town",              "Hatfield"),
        ("Letchworth Garden City Eagles","Letchworth"),
        ("Leighton Town Reserves",     "Leighton Res"),
        ("Luton Old Boys",             "Luton OB"),
        ("Mursley United",             "Mursley"),
        ("New Bradwell St Peter",      "New Bradwell"),
        ("Pitstone & Ivinghoe",        "Pitstone"),
        ("Sarratt",                    "Sarratt"),
        ("Totternhoe",                 "Totternhoe"),
        ("The 61 FC Luton",            "61 FC Luton"),
        ("Winslow United",             "Winslow"),
    ],

    # ── United Counties League Division One ──────────────────────────────
    "United Counties League Division One": [
        ("Birstall United",            "Birstall"),
        ("Burton Latimer Town",        "Burton Lat"),
        ("Cottingham",                 "Cottingham"),
        ("Earl Shilton Town",          "Earl Shilton"),
        ("Histon",                     "Histon"),
        ("Irthlingborough Diamonds",   "Irthlingborough"),
        ("Lutterworth Town",           "Lutterworth T"),
        ("Market Overton",             "Market Overton"),
        ("Moulton",                    "Moulton"),
        ("Oakham United",              "Oakham"),
        ("Oadby Town",                 "Oadby"),
        ("Rushden & Higham Utd Res",   "Rushden H Res"),
        ("Sawtry",                     "Sawtry"),
        ("Thrapston Town",             "Thrapston"),
        ("Wellingborough Whitworth Res","W'boro W Res"),
        ("Wollaston",                  "Wollaston"),
    ],

    # ── Wessex Football League Division One ─────────────────────────────
    "Wessex Football League Division One": [
        ("AFC Portchester",            "Portchester"),
        ("Andover Town",               "Andover"),
        ("Baffins Milton Rovers Res",  "Baffins Res"),
        ("Bemerton Heath Reserves",    "Bemerton Res"),
        ("Bournemouth Sports",         "B'mouth Sports"),
        ("Downton",                    "Downton FC"),
        ("East Cowes Victoria Athletic","East Cowes"),
        ("Folland Sports",             "Folland"),
        ("Hythe & Dibden",             "Hythe & D"),
        ("Laverstock & Ford",          "Laverstock"),
        ("New Milton Town",            "New Milton"),
        ("Newport IOW",                "Newport IOW"),
        ("Romsey Town",                "Romsey"),
        ("Shaftesbury Town",           "Shaftesbury T"),
        ("Stockbridge",                "Stockbridge"),
        ("Verwood Town",               "Verwood"),
    ],

    # ── Western Football League Division One ────────────────────────────
    "Western Football League Division One": [
        ("AEK Boco",                   "AEK Boco"),
        ("Almondsbury",                "Almondsbury"),
        ("Ashton & Backwell United",   "Ashton & B"),
        ("Bridport",                   "Bridport"),
        ("Buckland Athletic",          "Buckland"),
        ("Burnham United",             "Burnham Utd"),
        ("Chard Town",                 "Chard"),
        ("Chipping Sodbury Town",      "Chipping Sod"),
        ("Cribbs",                     "Cribbs"),
        ("Devizes Town",               "Devizes"),
        ("Gillingham Town",            "Gillingham T"),
        ("Hallen",                     "Hallen"),
        ("Longwell Green Sports",      "Longwell Grn"),
        ("Portishead Town",            "Portishead"),
        ("Shirehampton",               "Shirehampton"),
        ("Warminster Town",            "Warminster"),
    ],
}


# ═══════════════════════════════════════════════════════════════════════════
# Step lookup — derived from league names in pyramid.py
# ═══════════════════════════════════════════════════════════════════════════

_LEAGUE_STEP: dict[str, int] = {}
_STEP_4 = [
    "Northern Premier League Division One East",
    "Northern Premier League Division One Midlands",
    "Northern Premier League Division One West",
    "Southern League Division One Central",
    "Southern League Division One South",
    "Isthmian League Division One North",
    "Isthmian League Division One South Central",
    "Isthmian League Division One South East",
]
_STEP_5 = [
    "Combined Counties League Premier Division North",
    "Combined Counties League Premier Division South",
    "Eastern Counties Football League Premier Division",
    "Essex Senior Football League",
    "Hellenic Football League Premier Division",
    "Midland Football League Premier Division",
    "North West Counties Football League Premier Division",
    "Northern Football League Division One",
    "Northern Counties East Football League Premier Division",
    "Southern Combination Football League Premier Division",
    "Southern Counties East Football League Premier Division",
    "Spartan South Midlands Football League Premier Division",
    "United Counties League Premier Division North",
    "United Counties League Premier Division South",
    "Wessex Football League Premier Division",
    "Western Football League Premier Division",
]
_STEP_6 = [
    "Combined Counties League Division One",
    "Eastern Counties Football League Division One North",
    "Eastern Counties Football League Division One South",
    "Hellenic Football League Division One",
    "Hellenic Football League Division Two",
    "Midland Football League Division One",
    "Midland Football League Division Two",
    "North West Counties Football League Division One North",
    "North West Counties Football League Division One South",
    "Northern Football League Division Two",
    "Northern Counties East Football League Division One",
    "Southern Combination Football League Division One",
    "Southern Combination Football League Division Two",
    "Southern Counties East Football League Division One",
    "Spartan South Midlands Football League Division One",
    "Spartan South Midlands Football League Division Two",
    "United Counties League Division One",
    "Wessex Football League Division One",
    "Western Football League Division One",
]

for _n in _STEP_4:
    _LEAGUE_STEP[_n] = 4
for _n in _STEP_5:
    _LEAGUE_STEP[_n] = 5
for _n in _STEP_6:
    _LEAGUE_STEP[_n] = 6


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _lookup_league_ids(session) -> dict[str, int]:
    """Return {league_name: league_id} for every league we reference."""
    names = list(CLUBS.keys())
    rows = session.execute(
        select(League.name, League.id).where(
            League.name.in_(names),
            League.season == SEASON,
        )
    ).all()
    return {name: lid for name, lid in rows}


def _batch_upsert(session, rows: list[dict]) -> None:
    """Upsert a batch of club rows in one statement."""
    if not rows:
        return
    stmt = pg_insert(Club).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["name"],
        set_={
            "short_name": stmt.excluded.short_name,
            "league_id": stmt.excluded.league_id,
            "is_active": stmt.excluded.is_active,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def load_clubs() -> dict[int, int]:
    """Seed clubs for Steps 4–6 of the 2024-25 pyramid.

    Requires the leagues table to be populated first
    (run ``python -m src.seeds.pyramid`` beforehand).

    Returns:
        A dict mapping step number to count of clubs upserted,
        e.g. ``{4: 160, 5: 288, 6: 304}``.
    """
    counts: dict[int, int] = {4: 0, 5: 0, 6: 0}

    with get_session() as session:
        league_ids = _lookup_league_ids(session)

        missing = [n for n in CLUBS if n not in league_ids]
        if missing:
            logger.error(
                "Cannot find these leagues in the DB (did you run pyramid.py first?): %s",
                missing,
            )
            raise RuntimeError(
                f"Missing leagues: {missing}. Run `python -m src.seeds.pyramid` first."
            )

        for league_name, club_list in CLUBS.items():
            lid = league_ids[league_name]
            step = _LEAGUE_STEP[league_name]

            batch = [
                dict(name=name, short_name=short, league_id=lid, is_active=True)
                for name, short in club_list
            ]
            _batch_upsert(session, batch)

            counts[step] += len(club_list)
            logger.info(
                "  %-55s  %3d clubs  (step %d, league_id=%d)",
                league_name, len(club_list), step, lid,
            )

    total = sum(counts.values())
    logger.info("Club seed complete — %d clubs upserted for Steps 4-6", total)
    for step in sorted(counts):
        logger.info("  Step %d: %d clubs", step, counts[step])

    return counts


# ═══════════════════════════════════════════════════════════════════════════
# CLI entry point:  python -m src.seeds.clubs_steps_4_6
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    counts = load_clubs()
    total = sum(counts.values())
    print(f"\nDone — {total} clubs seeded across Steps 4-6 for {SEASON}.")
