def test_iiif_to_metsmods():
    iiif_to_metsmods(
        manifesturl,
        session,
        newspaper,
        issues,
        alreadygeneratedids,
        logger,
        cwd,
        metsfolder,
    )
