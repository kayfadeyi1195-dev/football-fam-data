"""Seed the clubs table for Steps 1–3 of the 2024-25 English pyramid.

Covers:
    Step 1  – National League (24 clubs)
    Step 2  – National League North, National League South (~24 each)
    Step 3  – NPL Premier, Southern League Premier Central,
              Southern League Premier South, Isthmian League Premier
              (~22 each)

Total: approximately 160 clubs.

The script is **idempotent** — it uses PostgreSQL ``INSERT … ON CONFLICT
DO UPDATE`` keyed on the club ``name`` so re-runs update existing rows
rather than creating duplicates.

Usage::

    python -m src.seeds.clubs_steps_1_3
"""

import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.models import Club, League
from src.db.session import get_session

logger = logging.getLogger(__name__)

SEASON = "2024-25"

# ═══════════════════════════════════════════════════════════════════════════
# Club data, organised by league name.
#
# Each club tuple:
#   (name, short_name, website_url | None, pitchero_url | None)
#
# website_url is included where we are confident of the address.
# pitchero_url is set for clubs known to host on the Pitchero platform.
# ═══════════════════════════════════════════════════════════════════════════

CLUBS: dict[str, list[tuple[str, str, str | None, str | None]]] = {

    # �══════════════════════════════════════════════════════════════════════
    # STEP 1 — National League  (24 clubs)
    # ╚════════════════════════════════════════════════════════════════════╝

    "National League": [
        ("AFC Fylde",               "Fylde",       "https://www.afcfylde.co.uk",              None),
        ("Aldershot Town",          "Aldershot",   "https://www.theshots.co.uk",               None),
        ("Altrincham",              "Alty",        "https://www.altrinchamfc.co.uk",            None),
        ("Barnet",                  "Barnet",      "https://www.barnetfc.com",                  None),
        ("Boston United",           "Boston",      "https://www.bufc.co.uk",                    None),
        ("Braintree Town",          "Braintree",   "https://www.braintreetownfc.org.uk",        None),
        ("Dagenham & Redbridge",    "Dag & Red",   "https://www.dagandred.com",                 None),
        ("Eastleigh",               "Eastleigh",   "https://www.eastleighfc.com",               None),
        ("Ebbsfleet United",        "Ebbsfleet",   "https://www.ebbsfleetunited.co.uk",         None),
        ("FC Halifax Town",         "Halifax",     "https://www.fchalifaxtown.com",              None),
        ("Forest Green Rovers",     "FGR",         "https://www.fgr.co.uk",                     None),
        ("Gateshead",               "Gateshead",   "https://www.gateshead-fc.com",               None),
        ("Hartlepool United",       "Hartlepool",  "https://www.hartlepoolunited.co.uk",         None),
        ("Maidenhead United",       "Maidenhead",  "https://www.maidenheadunitedfc.co.uk",       None),
        ("Oldham Athletic",         "Oldham",      "https://www.oldhamathletic.co.uk",           None),
        ("Rochdale",                "Rochdale",    "https://www.rochdaleafc.co.uk",              None),
        ("Solihull Moors",          "Solihull",    "https://www.solihullmoorsfc.co.uk",           None),
        ("Southend United",         "Southend",    "https://www.southendunited.co.uk",           None),
        ("Sutton United",           "Sutton",      "https://www.suttonunited.net",               None),
        ("Tamworth",                "Tamworth",    "https://www.thelambs.co.uk",                 None),
        ("Wealdstone",              "Wealdstone",  "https://www.wealdstonefc.com",               None),
        ("Woking",                  "Woking",      "https://www.wokingfc.co.uk",                 None),
        ("Yeovil Town",             "Yeovil",      "https://www.ytfc.net",                       None),
        ("York City",               "York",        "https://www.yorkcityfootballclub.co.uk",     None),
    ],

    # ╚════════════════════════════════════════════════════════════════════╝
    # STEP 2 — National League North  (24 clubs)
    # ╚════════════════════════════════════════════════════════════════════╝

    "National League North": [
        ("Alfreton Town",           "Alfreton",        "https://www.alfretontownfc.com",        None),
        ("Banbury United",          "Banbury",         None,                                     None),
        ("Bishop's Stortford",      "Stortford",       "https://www.bishopsstortfordfc.com",     None),
        ("Brackley Town",           "Brackley",        "https://www.brackleytownfc.com",         None),
        ("Buxton",                  "Buxton",          "https://www.buxtonfc.co.uk",             "https://www.pitchero.com/clubs/buxtonfc"),
        ("Chester",                 "Chester",         "https://www.chesterfc.com",               None),
        ("Chorley",                 "Chorley",         "https://www.chorleyfc.com",               None),
        ("Curzon Ashton",           "Curzon",          "https://www.curzonashton.co.uk",          "https://www.pitchero.com/clubs/curzonashtonfc"),
        ("Darlington",              "Darlington",      "https://www.darlingtonfc.co.uk",          None),
        ("Gloucester City",         "Gloucester",      "https://www.gloucestercityafc.com",       None),
        ("Hereford",                "Hereford",        "https://www.herefordfc.co.uk",            None),
        ("Kettering Town",          "Kettering",       "https://www.ketteringtownfc.co.uk",       None),
        ("Kidderminster Harriers",  "Kiddy",           "https://www.harriers.co.uk",              None),
        ("King's Lynn Town",        "King's Lynn",     "https://www.kltown.co.uk",                None),
        ("Peterborough Sports",     "Peterborough S",  None,                                     "https://www.pitchero.com/clubs/peterboroughsports"),
        ("Rushall Olympic",         "Rushall",         None,                                     "https://www.pitchero.com/clubs/rushallolympic"),
        ("Scarborough Athletic",    "Scarborough",     "https://www.scarboroughathletic.com",     None),
        ("Scunthorpe United",       "Scunthorpe",      "https://www.scunthorpe-united.co.uk",     None),
        ("South Shields",           "South Shields",   "https://www.southshieldsfc.co.uk",        None),
        ("Southport",               "Southport",       "https://www.southportfc.net",              None),
        ("Spennymoor Town",         "Spennymoor",      "https://www.spennymoortown.co.uk",        None),
        ("AFC Telford United",      "Telford",         "https://www.telfordutd.co.uk",             None),
        ("Warrington Town",         "Warrington",      None,                                     "https://www.pitchero.com/clubs/warringtontown"),
        ("Whitby Town",             "Whitby",          None,                                     "https://www.pitchero.com/clubs/whitbytown"),
    ],

    # ╚════════════════════════════════════════════════════════════════════╝
    # STEP 2 — National League South  (24 clubs)
    # ╚════════════════════════════════════════════════════════════════════╝

    "National League South": [
        ("Aveley",                        "Aveley",      None,                                    "https://www.pitchero.com/clubs/aveleyfc"),
        ("Bath City",                     "Bath",        "https://www.bathcityfc.com",              None),
        ("Boreham Wood",                  "Boreham Wood","https://www.borehamwoodfc.co.uk",         None),
        ("Chelmsford City",               "Chelmsford",  "https://www.chelmsfordcityfc.com",        None),
        ("Chippenham Town",               "Chippenham",  None,                                    "https://www.pitchero.com/clubs/chippenhamtown"),
        ("Dartford",                      "Dartford",    "https://www.dartfordfc.co.uk",            None),
        ("Dorking Wanderers",             "Dorking",     "https://www.dorkingwanderersfc.co.uk",    None),
        ("Dover Athletic",                "Dover",       "https://www.doverathleticfc.com",         None),
        ("Farnborough",                   "Farnborough", "https://www.farnboroughfc.co.uk",         None),
        ("Hampton & Richmond Borough",    "Hampton",     "https://www.hamptonfc.net",               None),
        ("Havant & Waterlooville",        "Hawks",       "https://www.havantandwaterlooville.net",  None),
        ("Hemel Hempstead Town",          "Hemel",       "https://www.hemelfc.com",                 None),
        ("Maidstone United",              "Maidstone",   "https://www.maidstoneunited.co.uk",       None),
        ("Oxford City",                   "Oxford C",    "https://www.oxfordcityfc.co.uk",          None),
        ("St Albans City",                "St Albans",   "https://www.stalbanscityfc.com",          None),
        ("Slough Town",                   "Slough",      "https://www.sloughtownfc.net",            None),
        ("Taunton Town",                  "Taunton",     None,                                    "https://www.pitchero.com/clubs/tauntontown"),
        ("Tonbridge Angels",              "Tonbridge",   "https://www.tonbridgeangels.co.uk",       None),
        ("Torquay United",                "Torquay",     "https://www.torquayunited.com",           None),
        ("Truro City",                    "Truro",       "https://www.trurocityfc.co.uk",           None),
        ("Welling United",                "Welling",     "https://www.wellingunitedfc.co.uk",       None),
        ("Weston-super-Mare",             "Weston",      None,                                    "https://www.pitchero.com/clubs/westonsupermare"),
        ("Weymouth",                      "Weymouth",    "https://www.theterras.co.uk",             None),
        ("Worthing",                      "Worthing",    "https://www.worthingfc.com",              None),
    ],

    # ╚════════════════════════════════════════════════════════════════════╝
    # STEP 3 — Northern Premier League Premier Division  (22 clubs)
    # ╚════════════════════════════════════════════════════════════════════╝

    "Northern Premier League Premier Division": [
        ("Ashton United",           "Ashton Utd",     None,                                    "https://www.pitchero.com/clubs/ashtonunited"),
        ("Atherton Collieries",     "Atherton",       None,                                    "https://www.pitchero.com/clubs/athertoncollieries"),
        ("Bamber Bridge",           "Bamber Bridge",  None,                                    "https://www.pitchero.com/clubs/bamberbridge"),
        ("Basford United",          "Basford",        "https://www.basfordunitedfc.co.uk",      None),
        ("Blyth Spartans",          "Blyth",          "https://www.blythspartans.com",           None),
        ("FC United of Manchester", "FC Utd",         "https://www.fc-utd.co.uk",                None),
        ("Gainsborough Trinity",    "Gainsborough",   "https://www.gainsboroughtrinity.com",     None),
        ("Guiseley",                "Guiseley",       None,                                    "https://www.pitchero.com/clubs/guiseleyafc"),
        ("Hyde United",             "Hyde",           None,                                    "https://www.pitchero.com/clubs/hydeunited"),
        ("Ilkeston Town",           "Ilkeston",       None,                                    "https://www.pitchero.com/clubs/ilkestontown"),
        ("Lancaster City",          "Lancaster",      None,                                    "https://www.pitchero.com/clubs/lancastercity"),
        ("Leek Town",               "Leek",           None,                                    "https://www.pitchero.com/clubs/leektownfc"),
        ("Macclesfield",            "Macclesfield",   "https://www.macclesfieldfc.co.uk",        None),
        ("Marine",                  "Marine",         "https://www.marinefc.com",                None),
        ("Matlock Town",            "Matlock",        None,                                    "https://www.pitchero.com/clubs/matlocktownfc"),
        ("Morpeth Town",            "Morpeth",        None,                                    "https://www.pitchero.com/clubs/morpethtown"),
        ("Nantwich Town",           "Nantwich",       None,                                    "https://www.pitchero.com/clubs/nantwichtownfc"),
        ("Radcliffe",               "Radcliffe",      None,                                    "https://www.pitchero.com/clubs/radcliffeborough"),
        ("Stalybridge Celtic",      "Stalybridge",    "https://www.stalybridgeceltic.co.uk",     None),
        ("Stockton Town",           "Stockton",       "https://www.stocktontownfc.co.uk",        None),
        ("Warrington Rylands",      "Rylands",        None,                                    "https://www.pitchero.com/clubs/warringtonrylands1906"),
        ("Witton Albion",           "Witton",         "https://www.wittonalbion.com",             None),
    ],

    # ╚════════════════════════════════════════════════════════════════════╝
    # STEP 3 — Southern League Premier Division Central  (22 clubs)
    # ╚════════════════════════════════════════════════════════════════════╝

    "Southern League Premier Division Central": [
        ("AFC Rushden & Diamonds",  "Rushden",       None,                                     None),
        ("Alvechurch",              "Alvechurch",    None,                                     "https://www.pitchero.com/clubs/alvechurchfc"),
        ("Barwell",                 "Barwell",       None,                                     "https://www.pitchero.com/clubs/barwellfc"),
        ("Biggleswade Town",        "Biggleswade",   None,                                     "https://www.pitchero.com/clubs/biggleswadetown"),
        ("Bromsgrove Sporting",     "Bromsgrove",    "https://www.bromsgrove-sporting.co.uk",   None),
        ("Chasetown",               "Chasetown",     None,                                     "https://www.pitchero.com/clubs/chasetownfc"),
        ("Coalville Town",          "Coalville",     "https://www.coalvilletownfc.co.uk",        None),
        ("Corby Town",              "Corby",         None,                                     "https://www.pitchero.com/clubs/corbytown"),
        ("Hednesford Town",         "Hednesford",    "https://www.hednesfordtownfc.co.uk",       None),
        ("Hitchin Town",            "Hitchin",       "https://www.hitchintownfc.co.uk",          None),
        ("Leiston",                 "Leiston",       None,                                     "https://www.pitchero.com/clubs/leiston"),
        ("Lowestoft Town",          "Lowestoft",     "https://www.lowestofttownfc.co.uk",        None),
        ("Mickleover",              "Mickleover",    None,                                     "https://www.pitchero.com/clubs/mickleoversports"),
        ("Needham Market",          "Needham",       None,                                     "https://www.pitchero.com/clubs/needhammarketfc"),
        ("Nuneaton Borough",        "Nuneaton",      "https://www.nuneatonboroughfc.com",        None),
        ("Redditch United",         "Redditch",      "https://www.redditchunitedfc.co.uk",       None),
        ("Royston Town",            "Royston",       None,                                     "https://www.pitchero.com/clubs/roystontownfc"),
        ("St Ives Town",            "St Ives",       None,                                     "https://www.pitchero.com/clubs/stivestown"),
        ("St Neots Town",           "St Neots",      None,                                     "https://www.pitchero.com/clubs/stneotstown"),
        ("Stourbridge",             "Stourbridge",   "https://www.stourbridgefc.com",            None),
        ("Stamford AFC",            "Stamford",      None,                                     "https://www.pitchero.com/clubs/stamfordafc"),
        ("Stratford Town",          "Stratford",     None,                                     "https://www.pitchero.com/clubs/stratfordtownfc"),
    ],

    # ╚════════════════════════════════════════════════════════════════════╝
    # STEP 3 — Southern League Premier Division South  (22 clubs)
    # ╚════════════════════════════════════════════════════════════════════╝

    "Southern League Premier Division South": [
        ("AFC Totton",              "Totton",        None,                                     "https://www.pitchero.com/clubs/afctotton"),
        ("Beaconsfield Town",       "Beaconsfield",  None,                                     "https://www.pitchero.com/clubs/beaconsfieldtown"),
        ("Chesham United",          "Chesham",       "https://www.cheshamunitedfc.co.uk",        None),
        ("Frome Town",              "Frome",         None,                                     "https://www.pitchero.com/clubs/frometownfc"),
        ("Gosport Borough",         "Gosport",       None,                                     "https://www.pitchero.com/clubs/gosportborough"),
        ("Harrow Borough",          "Harrow",        None,                                     "https://www.pitchero.com/clubs/harrowborough"),
        ("Hartley Wintney",         "Hartley W",     None,                                     "https://www.pitchero.com/clubs/hartleywintney"),
        ("Hayes & Yeading United",  "Hayes",         "https://www.hayesandyeading.co.uk",       None),
        ("Hendon",                  "Hendon",        "https://www.hendonfc.net",                 None),
        ("Hungerford Town",         "Hungerford",    None,                                     "https://www.pitchero.com/clubs/hungerfordtownfc"),
        ("Kings Langley",           "Kings Langley", None,                                     "https://www.pitchero.com/clubs/kingslangley"),
        ("Merthyr Town",            "Merthyr",       "https://www.merthyrtownfc.co.uk",          None),
        ("Metropolitan Police",     "Met Police",    None,                                     "https://www.pitchero.com/clubs/metropolitanpolicefc"),
        ("Plymouth Parkway",        "Parkway",       None,                                     "https://www.pitchero.com/clubs/plymouthparkway"),
        ("Poole Town",              "Poole",         None,                                     "https://www.pitchero.com/clubs/pooletownfc"),
        ("Salisbury",               "Salisbury",     "https://www.salisburyfc.co.uk",            None),
        ("Swindon Supermarine",     "Supermarine",   None,                                     "https://www.pitchero.com/clubs/swindonsupermarinefc"),
        ("Tiverton Town",           "Tiverton",      None,                                     "https://www.pitchero.com/clubs/tivertontownfc"),
        ("Uxbridge",                "Uxbridge",      None,                                     "https://www.pitchero.com/clubs/uxbridgefc"),
        ("Walton & Hersham",        "Walton",        None,                                     "https://www.pitchero.com/clubs/waltonandhersham"),
        ("Wimborne Town",           "Wimborne",      None,                                     "https://www.pitchero.com/clubs/wimbornetownfc"),
        ("Yate Town",               "Yate",          None,                                     "https://www.pitchero.com/clubs/yatetownfc"),
    ],

    # ╚════════════════════════════════════════════════════════════════════╝
    # STEP 3 — Isthmian League Premier Division  (22 clubs)
    # ╚════════════════════════════════════════════════════════════════════╝

    "Isthmian League Premier Division": [
        ("AFC Hornchurch",          "Hornchurch",    None,                                     "https://www.pitchero.com/clubs/afchornchurch"),
        ("Billericay Town",         "Billericay",    "https://www.billericaytownfc.co.uk",       None),
        ("Bognor Regis Town",       "Bognor",        "https://www.bognorregistownfc.co.uk",      None),
        ("Bowers & Pitsea",         "Bowers",        None,                                     "https://www.pitchero.com/clubs/bowerspitsea"),
        ("Canvey Island",           "Canvey",        None,                                     "https://www.pitchero.com/clubs/canveyisland"),
        ("Carshalton Athletic",     "Carshalton",    "https://www.carshaltonathletic.co.uk",     None),
        ("Chatham Town",            "Chatham",       None,                                     "https://www.pitchero.com/clubs/chathamtown"),
        ("Cheshunt",                "Cheshunt",      None,                                     "https://www.pitchero.com/clubs/cheshuntfc"),
        ("Corinthian Casuals",      "Casuals",       "https://www.corinthian-casuals.com",       None),
        ("Cray Wanderers",          "Cray",          "https://www.cray-wanderers.com",           None),
        ("Dulwich Hamlet",          "Dulwich",       "https://www.dulwichhamletfc.co.uk",        None),
        ("Enfield Town",            "Enfield",       "https://www.enfieldtownfc.co.uk",          None),
        ("Folkestone Invicta",      "Folkestone",    "https://www.folkestoneinvicta.co.uk",      None),
        ("Haringey Borough",        "Haringey",      None,                                     "https://www.pitchero.com/clubs/haringeyborough"),
        ("Hastings United",         "Hastings",      "https://www.hastingsunitedfc.co.uk",       None),
        ("Horsham",                 "Horsham",       "https://www.horshamfc.co.uk",              None),
        ("Kingstonian",             "Kingstonian",   "https://www.kingstonian.com",              None),
        ("Lewes",                   "Lewes",         "https://www.lewesfc.com",                  None),
        ("Margate",                 "Margate",       "https://www.margatefc.com",                None),
        ("Potters Bar Town",        "Potters Bar",   None,                                     "https://www.pitchero.com/clubs/pottersbartown"),
        ("Whitehawk",               "Whitehawk",     None,                                     "https://www.pitchero.com/clubs/whitehawkfc"),
        ("Wingate & Finchley",      "Wingate",       "https://www.wingateandfinchley.com",       None),
    ],
}


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

# Maps league names → pyramid step for logging
_LEAGUE_STEP: dict[str, int] = {
    "National League": 1,
    "National League North": 2,
    "National League South": 2,
    "Northern Premier League Premier Division": 3,
    "Southern League Premier Division Central": 3,
    "Southern League Premier Division South": 3,
    "Isthmian League Premier Division": 3,
}


def _lookup_league_ids(session) -> dict[str, int]:
    """Return a {league_name: league_id} mapping for the leagues we need."""
    names = list(CLUBS.keys())
    rows = session.execute(
        select(League.name, League.id).where(
            League.name.in_(names),
            League.season == SEASON,
        )
    ).all()
    return {name: lid for name, lid in rows}


def _upsert_club(
    session,
    *,
    name: str,
    short_name: str,
    league_id: int | None,
    website_url: str | None,
    pitchero_url: str | None,
) -> None:
    """Insert a club or update it if the name already exists."""
    values = dict(
        name=name,
        short_name=short_name,
        league_id=league_id,
        website_url=website_url,
        pitchero_url=pitchero_url,
        is_active=True,
    )
    stmt = pg_insert(Club).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["name"],
        set_={
            "short_name": stmt.excluded.short_name,
            "league_id": stmt.excluded.league_id,
            "website_url": stmt.excluded.website_url,
            "pitchero_url": stmt.excluded.pitchero_url,
            "is_active": stmt.excluded.is_active,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def load_clubs() -> dict[int, int]:
    """Seed clubs for Steps 1–3 of the 2024-25 pyramid.

    Requires the leagues table to be populated first
    (run ``python -m src.seeds.pyramid`` beforehand).

    Returns:
        A dict mapping step number to count of clubs upserted,
        e.g. ``{1: 24, 2: 48, 3: 88}``.
    """
    counts: dict[int, int] = {1: 0, 2: 0, 3: 0}

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

            for club_name, short, website, pitchero in club_list:
                _upsert_club(
                    session,
                    name=club_name,
                    short_name=short,
                    league_id=lid,
                    website_url=website,
                    pitchero_url=pitchero,
                )
                counts[step] += 1

            logger.info(
                "  %-50s  %3d clubs upserted (league_id=%d)",
                league_name, len(club_list), lid,
            )

    total = sum(counts.values())
    logger.info("Club seed complete — %d clubs upserted for Steps 1-3", total)
    for step in sorted(counts):
        logger.info("  Step %d: %d clubs", step, counts[step])

    return counts


# ═══════════════════════════════════════════════════════════════════════════
# CLI entry point:  python -m src.seeds.clubs_steps_1_3
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    counts = load_clubs()
    total = sum(counts.values())
    print(f"\nDone — {total} clubs seeded across Steps 1-3 for {SEASON}.")
