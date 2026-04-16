from hr_hunter_transformer.company_quality import company_quality_score, looks_like_bad_company


def test_company_quality_rejects_generic_ui_and_fragment_values() -> None:
    assert looks_like_bad_company("Browse Jobs")
    assert looks_like_bad_company("Design & Project")
    assert looks_like_bad_company("Follow")
    assert looks_like_bad_company("Intern")
    assert looks_like_bad_company("Consultant")
    assert looks_like_bad_company("/ BIM Consultant")
    assert looks_like_bad_company("– ADREA")
    assert company_quality_score("– ADREA", "Architect", "design_architecture") == 0.0
    assert looks_like_bad_company("Procter &")
    assert looks_like_bad_company("Riyadh including the new Riyadh Metro")
    assert looks_like_bad_company("DAT & Partners Consultant Damascus University")
    assert looks_like_bad_company("Amar Golden Design AMGD United Arab Emirates University", "Architect")
    assert company_quality_score("U.S") < 0.18


def test_company_quality_keeps_real_architecture_employers() -> None:
    assert company_quality_score("Azizi Developments", "Architect", "design_architecture") >= 0.5
    assert company_quality_score("Killa Design I Architecture", "Architect", "design_architecture") >= 0.5
