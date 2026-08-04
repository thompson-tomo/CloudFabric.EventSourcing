import {Filter} from "../../src";
import {FilterOperator} from "../../src/queries/FilterOperator";

describe('Filter serialization', () => {
    it('Should correctly serialize and deserialize filter object', () => {
        let filter = new Filter('name', FilterOperator.StartsWithIgnoreCase, 'a');
        
        // вот это шлём на бэк и вставляем в квери стринг, оно безопасное, все символы экранированы
        let serializedString = filter.serialize();

        // потом можно сделать так:
        let filterDeserialized = Filter.deserialize(new URLSearchParams(window.location.search).get('filters'));
        const newFilter = filterDeserialized.and(new Filter('tags', FilterOperator.Contains, 'basketball'));
        
        expect(serializedString).toEqual('userId|eq|1|F|basic%20test%20filter|');

        let filterDeserialized = Filter.deserialize(serializedString);

        expect(filterDeserialized.propertyName).toEqual(filter.propertyName);
        expect(filterDeserialized.operator).toEqual(filter.operator);
        expect(filterDeserialized.value).toEqual(filter.value);
        expect(filterDeserialized.visible).toEqual(filter.visible);
        expect(filterDeserialized.tag).toEqual(filter.tag);
    });
    
    it('Should correctly serialize and deserialize filter object', () => {
        let filter = new Filter('userId', FilterOperator.Equal, 1);
        filter.tag = 'basic test filter';
        filter.visible = false;
        
        let serializedString = filter.serialize();

        expect(serializedString).toEqual('userId|eq|1|F|basic%20test%20filter|');
        
        let filterDeserialized = Filter.deserialize(serializedString);
        
        expect(filterDeserialized.propertyName).toEqual(filter.propertyName);
        expect(filterDeserialized.operator).toEqual(filter.operator);
        expect(filterDeserialized.value).toEqual(filter.value);
        expect(filterDeserialized.visible).toEqual(filter.visible);
        expect(filterDeserialized.tag).toEqual(filter.tag);
    });

    it('Should correctly serialize and deserialize filter object with nested filters', () => {
        let filter = new Filter('userId', FilterOperator.Equal, 1);
        filter.tag = 'basic test filter';
        filter.visible = false;
        
        filter.or(new Filter("age", FilterOperator.GreaterOrEqual, 18).and(new Filter('age', FilterOperator.LowerOrEqual, 25)));

        let serializedString = filter.serialize();

        expect(serializedString).toEqual('userId|eq|1|F|basic%20test%20filter|or+age|ge|18|F||and+age|le|25|F||');

        let filterDeserialized = Filter.deserialize(serializedString);

        expect(filterDeserialized.propertyName).toEqual(filter.propertyName);
        expect(filterDeserialized.operator).toEqual(filter.operator);
        expect(filterDeserialized.value).toEqual(filter.value);
        expect(filterDeserialized.visible).toEqual(filter.visible);
        expect(filterDeserialized.tag).toEqual(filter.tag);
        
        expect(serializedString).toEqual(filterDeserialized.serialize());
    });

    it('Should correctly serialize and deserialize filter object with string filters', () => {
        let filter = new Filter('userId', FilterOperator.Equal, '123');
        filter.tag = 'basic test filter';
        filter.visible = false;

        filter.or(new Filter("age", FilterOperator.GreaterOrEqual, 18).and(new Filter('age', FilterOperator.LowerOrEqual, 25)));

        let serializedString = filter.serialize();

        expect(serializedString).toEqual("userId|eq|'123'|F|basic%20test%20filter|or+age|ge|18|F||and+age|le|25|F||");

        let filterDeserialized = Filter.deserialize(serializedString);

        expect(filterDeserialized.propertyName).toEqual(filter.propertyName);
        expect(filterDeserialized.operator).toEqual(filter.operator);
        expect(filterDeserialized.value).toEqual(filter.value);
        expect(filterDeserialized.visible).toEqual(filter.visible);
        expect(filterDeserialized.tag).toEqual(filter.tag);

        expect(serializedString).toEqual(filterDeserialized.serialize());
    });
    
    it('', () => {
        let currentSport = {"id":"afc-east","tags":["Football","NFL","American Football Conference","AFC East"],"createdAt":"2025-03-04T13:59:11.959Z","name":"AFC East","path":"football/league/nfl/conference/american-football-conference/division/afc-east","parentPath":"football/league/nfl/conference/american-football-conference","parentCollectionName":"division","breadcrumbs":["Football","NFL","American Football Conference","AFC East"],"assignedEntityType":"team"};

        const filter = currentSport?.tags?.length
            ? currentSport.tags.reduce<Filter | null>((acc, tag) => {
            const newFilter = new Filter(
                'sports',
                FilterOperator.ArrayContains,
                `'${tag}'`,
            )
            return acc ? acc.and(newFilter) : newFilter
        }, null) || undefined
            : undefined
        
        const serialized = filter.serialize();

        expect(serialized).toEqual("sports|array-contains|'%3Baps%3BFootball%3Baps%3B'|F||and+sports|array-contains|'%3Baps%3BNFL%3Baps%3B'|F||.and+sports|array-contains|'%3Baps%3BAmerican%20Football%20Conference%3Baps%3B'|F||.and+sports|array-contains|'%3Baps%3BAFC%20East%3Baps%3B'|F||");

        let filterDeserialized = Filter.deserialize(serialized);
    });
    
    it('adsf', () => {
        const filter = new Filter()

        if ("fdsa") {
            let alphabetFilter = filter.filters.find(
                (filter) => filter.filter.tag === 'alphabet-filter',
            )?.filter

            if (!alphabetFilter) {
                alphabetFilter = new Filter(
                    'name',
                    FilterOperator.StartsWith,
                    null,
                    'alphabet-filter',
                )
                filter.and(alphabetFilter)
            }

            alphabetFilter.value = 'A'
        } else {
            filter.filters = filter.filters.filter(
                (filter) => filter.filter.tag !== 'alphabet-filter',
            )
        }

        if ("vcxzvcxz") {
            let sportsFilter = filter.filters.find(
                (filter) => filter.filter.tag === 'sports-filter',
            )?.filter

            if (!sportsFilter) {
                sportsFilter = new Filter(
                    'tags',
                    FilterOperator.ArrayContains,
                    null,
                    'sports-filter',
                )
                filter.and(sportsFilter)
            }

            sportsFilter.value = 'basketball'
        } else {
            filter.filters = filter.filters.filter(
                (filter) => filter.filter.tag !== 'sports-filter',
            )
        }
        
        const serialized = filter.serialize();

        let filterDeserialized = Filter.deserialize(serialized);

        expect(filter).toEqual(filterDeserialized);
    });
});