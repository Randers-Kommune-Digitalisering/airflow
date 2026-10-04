from typing import Any

from dag_xflow_nexus_hjaelpemidler.models import Field, FieldType


def generate_personligt_hjaelpemiddel_fields(model_object: Any) -> list[Field]:
    communication_source = model_object.on_behalf_of_relation if model_object.for_another else "Borger"
    optional_contact_info = (
        f"{(model_object.on_behalf_of_text or '').lower()}\n"
        f"{model_object.on_behalf_of_name} - tlf: {model_object.on_behalf_of_phone}"
        if model_object.for_another and model_object.on_behalf_of_relation
        else None
    )

    fields = [
        Field(label="Ansøgningsdato", field_type=FieldType.DATE, value=model_object.form_date),
        Field(label="Henvendelses årsag", field_type=FieldType.TEXT_AREA, value=model_object.reason_text),
        Field(label="Henvendelseskilde", field_type=FieldType.DROPDOWN, value=communication_source),
        Field(label="Er borgeren indforstået med henvendelsen?", field_type=FieldType.DROPDOWN, value="Ja"),
        Field(
            label="Der er givet tilladelse til indhentning af oplysninger",
            field_type=FieldType.DROPDOWN,
            value="Ja" if model_object.can_collect_data else "Nej",
        ),
        Field(label="Borger oplyst om ovenstående", field_type=FieldType.DROPDOWN, value="Skriftligt"),
        Field(label="Hvad søges der om?", field_type=FieldType.CHECK_GROUP, value=model_object.device_name, require_match=False),
    ]

    if optional_contact_info:
        fields.append(Field(label="Uddyb med navn, telefonnummer m.m.", field_type=FieldType.TEXT_AREA, value=optional_contact_info))

    return fields
