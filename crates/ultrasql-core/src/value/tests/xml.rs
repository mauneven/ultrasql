use super::super::*;

    #[test]
    fn xml_validator_accepts_balanced_document_and_rejects_open_tag() {
        assert_eq!(
            Value::validate_xml_text(r#"<root attr="v"><child>text</child></root>"#),
            Some(r#"<root attr="v"><child>text</child></root>"#.to_owned())
        );
        assert_eq!(
            Value::validate_xml_text(r#"<?xml version="1.0"?><root><copy/></root>"#),
            Some(r#"<?xml version="1.0"?><root><copy/></root>"#.to_owned())
        );
        assert_eq!(Value::validate_xml_text("<root>"), None);
        assert_eq!(Value::validate_xml_text("<root attr=v/>"), None);
        assert_eq!(Value::validate_xml_text("<a/><b/>"), None);
    }

    #[test]
    fn xml_xpath_subset_filters_children_without_entity_resolution() {
        let doc = r#"<root><item id="1"><name>A</name></item><item id="2"><name>B</name></item><empty/></root>"#;
        assert_eq!(
            xml_xpath_element_fragments(r#"/root/item[@id="2"]/name"#, doc),
            Some(vec!["<name>B</name>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item/@id", doc),
            Some(vec!["1".to_owned(), "2".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item/name/text()", doc),
            Some(vec!["A".to_owned(), "B".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/*", doc),
            Some(vec![
                r#"<item id="1"><name>A</name></item>"#.to_owned(),
                r#"<item id="2"><name>B</name></item>"#.to_owned(),
                "<empty/>".to_owned(),
            ])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/*/@*", doc),
            Some(vec!["1".to_owned(), "2".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("count(/root/item)", doc),
            Some(vec!["2".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("string(/root/item/name)", doc),
            Some(vec!["A".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("boolean(/root/item)", doc),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("true()", doc),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("false()", doc),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("not(/root/missing)", doc),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("not(/root/item)", doc),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("name(/root/item)", doc),
            Some(vec!["item".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("local-name(/root/item)", doc),
            Some(vec!["item".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "normalize-space(/root/item)",
                r#"<root><item>  Ada   Lovelace </item></root>"#
            ),
            Some(vec!["Ada Lovelace".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "string-length(/root/item)",
                r#"<root><item>  Ada   Lovelace </item></root>"#
            ),
            Some(vec!["17".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"contains(/root/item, "Ada")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"contains(/root/item, "Turing")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"starts-with(/root/item, "Ada")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["true".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"starts-with(/root/missing, "Ada")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["false".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"substring-before(/root/item, " ")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["Ada".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"substring-after(/root/item, " ")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["Lovelace".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "substring(/root/item, 5)",
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["Lovelace".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "substring(/root/item, 1, 3)",
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec!["Ada".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"substring-before(/root/item, "x")"#,
                r#"<root><item>Ada Lovelace</item></root>"#
            ),
            Some(vec![String::new()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"translate(/root/item, "abc", "ABC")"#,
                r#"<root><item>database</item></root>"#
            ),
            Some(vec!["dAtABAse".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"translate(/root/item, "ae", "")"#,
                r#"<root><item>database</item></root>"#
            ),
            Some(vec!["dtbs".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"concat(/root/first, " ", /root/last)"#,
                r#"<root><first>Ada</first><last>Lovelace</last></root>"#
            ),
            Some(vec!["Ada Lovelace".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"concat("prefix-", /root/missing)"#,
                r#"<root><first>Ada</first></root>"#
            ),
            Some(vec!["prefix-".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "number(/root/value)",
                r#"<root><value> 42.5 </value></root>"#
            ),
            Some(vec!["42.5".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "floor(/root/value)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["42".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "ceiling(/root/value)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["43".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "number(/root/missing)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["NaN".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "round(/root/value)",
                r#"<root><value>42.5</value></root>"#
            ),
            Some(vec!["43".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "round(/root/value)",
                r#"<root><value>-42.5</value></root>"#
            ),
            Some(vec!["-42".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "sum(/root/value)",
                r#"<root><value>1.5</value><value>2.25</value></root>"#
            ),
            Some(vec!["3.75".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("sum(/root/missing)", r#"<root><value>1.5</value></root>"#),
            Some(vec!["0".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                "sum(/root/value)",
                r#"<root><value>1</value><value>bad</value></root>"#
            ),
            Some(vec!["NaN".to_owned()])
        );
        let positioned = r#"<root><item>a</item><item>b</item><item>c</item></root>"#;
        assert_eq!(
            xml_xpath_element_fragments("/root/item[position()=1]", positioned),
            Some(vec!["<item>a</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item[2]", positioned),
            Some(vec!["<item>b</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item[last()]", positioned),
            Some(vec!["<item>c</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/item[position()=last()]", positioned),
            Some(vec!["<item>c</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"/root/item[text()="a"]"#,
                r#"<root><item>a</item><item>b</item></root>"#
            ),
            Some(vec!["<item>a</item>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments(
                r#"/root/item[name="B"]"#,
                r#"<root><item><name>A</name></item><item><name>B</name></item></root>"#
            ),
            Some(vec!["<item><name>B</name></item>".to_owned()])
        );
        let nested = r#"<root><group><item id="1"><name>A</name></item><item id="2"><name>B</name></item></group><name>C</name></root>"#;
        assert_eq!(
            xml_xpath_element_fragments(r#"//item[@id="2"]/name"#, nested),
            Some(vec!["<name>B</name>".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/root//name", nested),
            Some(vec![
                "<name>A</name>".to_owned(),
                "<name>B</name>".to_owned(),
                "<name>C</name>".to_owned()
            ])
        );
        let namespaced =
            r#"<r:root xmlns:r="urn:r" xmlns:x="urn:x"><r:item x:id="7">Z</r:item></r:root>"#;
        assert_eq!(
            xml_xpath_element_fragments("/r:root/r:item/@x:id", namespaced),
            Some(vec!["7".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/r:root/r:item/text()", namespaced),
            Some(vec!["Z".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("local-name(/r:root/r:item)", namespaced),
            Some(vec!["item".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments("/r:root/@*", namespaced),
            Some(Vec::new())
        );
        assert_eq!(
            xml_xpath_element_fragments("/root/missing", doc),
            Some(Vec::new())
        );
        assert_eq!(xml_xpath_element_fragments("/root/..", doc), None);
        assert!(Value::xml_content_is_well_formed("<a/><b/>"));
        assert!(!Value::xml_content_is_well_formed("&unknown;"));
        assert!(!Value::xml_document_is_well_formed(
            r#"<!DOCTYPE root [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><root/>"#
        ));
    }

    #[test]
    fn xml_xpath_subset_resolves_namespace_uri_aliases() {
        let doc =
            r#"<root xmlns="urn:root" xmlns:i="urn:item"><i:child i:id="7">z</i:child></root>"#;
        let namespaces = vec![
            ("r".to_owned(), "urn:root".to_owned()),
            ("item".to_owned(), "urn:item".to_owned()),
        ];

        assert_eq!(
            xml_xpath_element_fragments_with_namespaces(
                "/r:root/item:child/@item:id",
                doc,
                &namespaces
            ),
            Some(vec!["7".to_owned()])
        );
        assert_eq!(
            xml_xpath_element_fragments_with_namespaces(
                "/r:root/item:child/text()",
                doc,
                &namespaces
            ),
            Some(vec!["z".to_owned()])
        );
    }

