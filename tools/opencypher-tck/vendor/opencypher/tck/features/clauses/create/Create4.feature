#
# Copyright (c) 2015-2023 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Attribution Notice under the terms of the Apache License 2.0
#
# This work was created by the collective efforts of the openCypher community.
# Without limiting the terms of Section 6, any Derivative Work that is not
# approved by the public consensus process of the openCypher Implementers Group
# should not be described as “Cypher” (and Cypher® is a registered trademark of
# Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
# proposals for change that have been documented or implemented should only be
# described as "implementation extensions to Cypher" or as "proposed changes to
# Cypher that are not yet approved by the openCypher community".
#

#encoding: utf-8

Feature: Create4 - Large Create Query

  Scenario: [1] Generate the movie graph
    Given an empty graph
    When executing query:
      """
      CREATE (theMatrix:Movie {title: 'The Matrix', released: 1999, tagline: 'Welcome to the Real World'})
      CREATE (keanu:Person {name: 'Keanu Reeves', born: 1964})
      CREATE (carrie:Person {name: 'Carrie-Anne Moss', born: 1967})
      CREATE (laurence:Person {name: 'Laurence Fishburne', born: 1961})
      CREATE (hugo:Person {name: 'Hugo Weaving', born: 1960})
      CREATE (andyW:Person {name: 'Andy Wachowski', born: 1967})
      CREATE (lanaW:Person {name: 'Lana Wachowski', born: 1965})
      CREATE (joelS:Person {name: 'Joel Silver', born: 1952})
      CREATE
        (keanu)-[:ACTED_IN {roles: ['Neo']}]->(theMatrix),
        (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(theMatrix),
        (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(theMatrix),
        (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(theMatrix),
        (andyW)-[:DIRECTED]->(theMatrix),
        (lanaW)-[:DIRECTED]->(theMatrix),
        (joelS)-[:PRODUCED]->(theMatrix)

      CREATE (emil:Person {name: 'Emil Eifrem', born: 1978})
      CREATE (emil)-[:ACTED_IN {roles: ['Emil']}]->(theMatrix)

      CREATE (theMatrixReloaded:Movie {title: 'The Matrix Reloaded', released: 2003,
              tagline: 'Free your mind'})
      CREATE
        (keanu)-[:ACTED_IN {roles: ['Neo'] }]->(theMatrixReloaded),
        (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(theMatrixReloaded),
        (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(theMatrixReloaded),
        (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(theMatrixReloaded),
        (andyW)-[:DIRECTED]->(theMatrixReloaded),
        (lanaW)-[:DIRECTED]->(theMatrixReloaded),
        (joelS)-[:PRODUCED]->(theMatrixReloaded)

      CREATE (theMatrixRevolutions:Movie {title: 'The Matrix Revolutions', released: 2003,
        tagline: 'Everything that has a beginning has an end'})
      CREATE
        (keanu)-[:ACTED_IN {roles: ['Neo']}]->(theMatrixRevolutions),
        (carrie)-[:ACTED_IN {roles: ['Trinity']}]->(theMatrixRevolutions),
        (laurence)-[:ACTED_IN {roles: ['Morpheus']}]->(theMatrixRevolutions),
        (hugo)-[:ACTED_IN {roles: ['Agent Smith']}]->(theMatrixRevolutions),
        (andyW)-[:DIRECTED]->(theMatrixRevolutions),
        (lanaW)-[:DIRECTED]->(theMatrixRevolutions),
        (joelS)-[:PRODUCED]->(theMatrixRevolutions)

      CREATE (theDevilsAdvocate:Movie {title: 'The Devil\'s Advocate', released: 1997,
        tagline: 'Evil has its winning ways'})
      CREATE (charlize:Person {name: 'Charlize Theron', born: 1975})
      CREATE (al:Person {name: 'Al Pacino', born: 1940})
      CREATE (taylor:Person {name: 'Taylor Hackford', born: 1944})
      CREATE
        (keanu)-[:ACTED_IN {roles: ['Kevin Lomax']}]->(theDevilsAdvocate),
        (charlize)-[:ACTED_IN {roles: ['Mary Ann Lomax']}]->(theDevilsAdvocate),
        (al)-[:ACTED_IN {roles: ['John Milton']}]->(theDevilsAdvocate),
        (taylor)-[:DIRECTED]->(theDevilsAdvocate)

      CREATE (aFewGoodMen:Movie {title: 'A Few Good Men', released: 1992,
        tagline: 'Deep within the heart of the nation\'s capital, one man will stop at nothing to keep his honor, ...'})
      CREATE (tomC:Person {name: 'Tom Cruise', born: 1962})
      CREATE (jackN:Person {name: 'Jack Nicholson', born: 1937})
      CREATE (demiM:Person {name: 'Demi Moore', born: 1962})
      CREATE (kevinB:Person {name: 'Kevin Bacon', born: 1958})
      CREATE (kieferS:Person {name: 'Kiefer Sutherland', born: 1966})
      CREATE (noahW:Person {name: 'Noah Wyle', born: 1971})
      CREATE (cubaG:Person {name: 'Cuba Gooding Jr.', born: 1968})
      CREATE (kevinP:Person {name: 'Kevin Pollak', born: 1957})
      CREATE (jTW:Person {name: 'J.T. Walsh', born: 1943})
      CREATE (jamesM:Person {name: 'James Marshall', born: 1967})
      CREATE (christopherG:Person {name: 'Christopher Guest', born: 1948})
      CREATE (robR:Person {name: 'Rob Reiner', born: 1947})
      CREATE (aaronS:Person {name: 'Aaron Sorkin', born: 1961})
      CREATE
        (tomC)-[:ACTED_IN {roles: ['Lt. Daniel Kaffee']}]->(aFewGoodMen),
        (jackN)-[:ACTED_IN {roles: ['Col. Nathan R. Jessup']}]->(aFewGoodMen),
        (demiM)-[:ACTED_IN {roles: ['Lt. Cdr. JoAnne Galloway']}]->(aFewGoodMen),
        (kevinB)-[:ACTED_IN {roles: ['Capt. Jack Ross']}]->(aFewGoodMen),
        (kieferS)-[:ACTED_IN {roles: ['Lt. Jonathan Kendrick']}]->(aFewGoodMen),
        (noahW)-[:ACTED_IN {roles: ['Cpl. Jeffrey Barnes']}]->(aFewGoodMen),
        (cubaG)-[:ACTED_IN {roles: ['Cpl. Carl Hammaker']}]->(aFewGoodMen),
        (kevinP)-[:ACTED_IN {roles: ['Lt. Sam Weinberg']}]->(aFewGoodMen),
        (jTW)-[:ACTED_IN {roles: ['Lt. Col. Matthew Andrew Markinson']}]->(aFewGoodMen),
        (jamesM)-[:ACTED_IN {roles: ['Pfc. Louden Downey']}]->(aFewGoodMen),
        (christopherG)-[:ACTED_IN {roles: ['Dr. Stone']}]->(aFewGoodMen),
        (aaronS)-[:ACTED_IN {roles: ['Bar patron']}]->(aFewGoodMen),
        (robR)-[:DIRECTED]->(aFewGoodMen),
        (aaronS)-[:WROTE]->(aFewGoodMen)

      CREATE (topGun:Movie {title: 'Top Gun', released: 1986,
          tagline: 'I feel the need, the need for speed.'})
      CREATE (kellyM:Person {name: 'Kelly McGillis', born: 1957})
      CREATE (valK:Person {name: 'Val Kilmer', born: 1959})
      CREATE (anthonyE:Person {name: 'Anthony Edwards', born: 1962})
      CREATE (tomS:Person {name: 'Tom Skerritt', born: 1933})
      CREATE (megR:Person {name: 'Meg Ryan', born: 1961})
      CREATE (tonyS:Person {name: 'Tony Scott', born: 1944})
      CREATE (jimC:Person {name: 'Jim Cash', born: 1941})
      CREATE
        (tomC)-[:ACTED_IN {roles: ['Maverick']}]->(topGun),
        (kellyM)-[:ACTED_IN {roles: ['Charlie']}]->(topGun),
        (valK)-[:ACTED_IN {roles: ['Iceman']}]->(topGun),
        (anthonyE)-[:ACTED_IN {roles: ['Goose']}]->(topGun),
        (tomS)-[:ACTED_IN {roles: ['Viper']}]->(topGun),
        (megR)-[:ACTED_IN {roles: ['Carole']}]->(topGun),
        (tonyS)-[:DIRECTED]->(topGun),
        (jimC)-[:WROTE]->(topGun)

      CREATE (jerryMaguire:Movie {title: 'Jerry Maguire', released: 2000,
          tagline: 'The rest of his life begins now.'})
      CREATE (reneeZ:Person {name: 'Renee Zellweger', born: 1969})
      CREATE (kellyP:Person {name: 'Kelly Preston', born: 1962})
      CREATE (jerryO:Person {name: 'Jerry O\'Connell', born: 1974})
      CREATE (jayM:Person {name: 'Jay Mohr', born: 1970})
      CREATE (bonnieH:Person {name: 'Bonnie Hunt', born: 1961})
      CREATE (reginaK:Person {name: 'Regina King', born: 1971})
      CREATE (jonathanL:Person {name: 'Jonathan Lipnicki', born: 1996})
      CREATE (cameronC:Person {name: 'Cameron Crowe', born: 1957})
      CREATE
        (tomC)-[:ACTED_IN {roles: ['Jerry Maguire']}]->(jerryMaguire),
        (cubaG)-[:ACTED_IN {roles: ['Rod Tidwell']}]->(jerryMaguire),
        (reneeZ)-[:ACTED_IN {roles: ['Dorothy Boyd']}]->(jerryMaguire),
        (kellyP)-[:ACTED_IN {roles: ['Avery Bishop']}]->(jerryMaguire),
        (jerryO)-[:ACTED_IN {roles: ['Frank Cushman']}]->(jerryMaguire),
        (jayM)-[:ACTED_IN {roles: ['Bob Sugar']}]->(jerryMaguire),
        (bonnieH)-[:ACTED_IN {roles: ['Laurel Boyd']}]->(jerryMaguire),
        (reginaK)-[:ACTED_IN {roles: ['Marcee Tidwell']}]->(jerryMaguire),
        (jonathanL)-[:ACTED_IN {roles: ['Ray Boyd']}]->(jerryMaguire),
        (cameronC)-[:DIRECTED]->(jerryMaguire),
        (cameronC)-[:PRODUCED]->(jerryMaguire),
        (cameronC)-[:WROTE]->(jerryMaguire)

      CREATE (standByMe:Movie {title: 'Stand-By-Me', released: 1986,
          tagline: 'The last real taste of innocence'})
      CREATE (riverP:Person {name: 'River Phoenix', born: 1970})
      CREATE (coreyF:Person {name: 'Corey Feldman', born: 1971})
      CREATE (wilW:Person {name: 'Wil Wheaton', born: 1972})
      CREATE (johnC:Person {name: 'John Cusack', born: 1966})
      CREATE (marshallB:Person {name: 'Marshall Bell', born: 1942})
      CREATE
        (wilW)-[:ACTED_IN {roles: ['Gordie Lachance']}]->(standByMe),
        (riverP)-[:ACTED_IN {roles: ['Chris Chambers']}]->(standByMe),
        (jerryO)-[:ACTED_IN {roles: ['Vern Tessio']}]->(standByMe),
        (coreyF)-[:ACTED_IN {roles: ['Teddy Duchamp']}]->(standByMe),
        (johnC)-[:ACTED_IN {roles: ['Denny Lachance']}]->(standByMe),
        (kieferS)-[:ACTED_IN {roles: ['Ace Merrill']}]->(standByMe),
        (marshallB)-[:ACTED_IN {roles: ['Mr. Lachance']}]->(standByMe),
        (robR)-[:DIRECTED]->(standByMe)

      CREATE (asGoodAsItGets:Movie {title: 'As-good-as-it-gets', released: 1997,
          tagline: 'A comedy from the heart that goes for the throat'})
      CREATE (helenH:Person {name: 'Helen Hunt', born: 1963})
      CREATE (gregK:Person {name: 'Greg Kinnear', born: 1963})
      CREATE (jamesB:Person {name: 'James L. Brooks', born: 1940})
      CREATE
        (jackN)-[:ACTED_IN {roles: ['Melvin Udall']}]->(asGoodAsItGets),
        (helenH)-[:ACTED_IN {roles: ['Carol Connelly']}]->(asGoodAsItGets),
        (gregK)-[:ACTED_IN {roles: ['Simon Bishop']}]->(asGoodAsItGets),
        (cubaG)-[:ACTED_IN {roles: ['Frank Sachs']}]->(asGoodAsItGets),
        (jamesB)-[:DIRECTED]->(asGoodAsItGets)

      CREATE (whatDreamsMayCome:Movie {title: 'What Dreams May Come', released: 1998,
          tagline: 'After life there is more. The end is just the beginning.'})
      CREATE (annabellaS:Person {name: 'Annabella Sciorra', born: 1960})
      CREATE (maxS:Person {name: 'Max von Sydow', born: 1929})
      CREATE (wernerH:Person {name: 'Werner Herzog', born: 1942})
      CREATE (robin:Person {name: 'Robin Williams', born: 1951})
      CREATE (vincentW:Person {name: 'Vincent Ward', born: 1956})
      CREATE
        (robin)-[:ACTED_IN {roles: ['Chris Nielsen']}]->(whatDreamsMayCome),
        (cubaG)-[:ACTED_IN {roles: ['Albert Lewis']}]->(whatDreamsMayCome),
        (annabellaS)-[:ACTED_IN {roles: ['Annie Collins-Nielsen']}]->(whatDreamsMayCome),
        (maxS)-[:ACTED_IN {roles: ['The Tracker']}]->(whatDreamsMayCome),
        (wernerH)-[:ACTED_IN {roles: ['The Face']}]->(whatDreamsMayCome),
        (vincentW)-[:DIRECTED]->(whatDreamsMayCome)

      CREATE (snowFallingonCedars:Movie {title: 'Snow-Falling-on-Cedars', released: 1999,
        tagline: 'First loves last. Forever.'})
      CREATE (ethanH:Person {name: 'Ethan Hawke', born: 1970})
      CREATE (rickY:Person {name: 'Rick Yune', born: 1971})
      CREATE (jamesC:Person {name: 'James Cromwell', born: 1940})
      CREATE (scottH:Person {name: 'Scott Hicks', born: 1953})
      CREATE
        (ethanH)-[:ACTED_IN {roles: ['Ishmael Chambers']}]->(snowFallingonCedars),
        (rickY)-[:ACTED_IN {roles: ['Kazuo Miyamoto']}]->(snowFallingonCedars),
        (maxS)-[:ACTED_IN {roles: ['Nels Gudmundsson']}]->(snowFallingonCedars),
        (jamesC)-[:ACTED_IN {roles: ['Judge Fielding']}]->(snowFallingonCedars),
        (scottH)-[:DIRECTED]->(snowFallingonCedars)

      CREATE (youveGotMail:Movie {title: 'You\'ve Got Mail', released: 1998,
          tagline: 'At-odds-in-life, in-love-on-line'})
      CREATE (parkerP:Person {name: 'Parker Posey', born: 1968})
      CREATE (daveC:Person {name: 'Dave Chappelle', born: 1973})
      CREATE (steveZ:Person {name: 'Steve Zahn', born: 1967})
      CREATE (tomH:Person {name: 'Tom Hanks', born: 1956})
      CREATE (noraE:Person {name: 'Nora Ephron', born: 1941})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Joe Fox']}]->(youveGotMail),
        (megR)-[:ACTED_IN {roles: ['Kathleen Kelly']}]->(youveGotMail),
        (gregK)-[:ACTED_IN {roles: ['Frank Navasky']}]->(youveGotMail),
        (parkerP)-[:ACTED_IN {roles: ['Patricia Eden']}]->(youveGotMail),
        (daveC)-[:ACTED_IN {roles: ['Kevin Jackson']}]->(youveGotMail),
        (steveZ)-[:ACTED_IN {roles: ['George Pappas']}]->(youveGotMail),
        (noraE)-[:DIRECTED]->(youveGotMail)

      CREATE (sleeplessInSeattle:Movie {title: 'Sleepless-in-Seattle', released: 1993,
          tagline: 'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})
      CREATE (ritaW:Person {name: 'Rita Wilson', born: 1956})
      CREATE (billPull:Person {name: 'Bill Pullman', born: 1953})
      CREATE (victorG:Person {name: 'Victor Garber', born: 1949})
      CREATE (rosieO:Person {name: 'Rosie O\'Donnell', born: 1962})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Sam Baldwin']}]->(sleeplessInSeattle),
        (megR)-[:ACTED_IN {roles: ['Annie Reed']}]->(sleeplessInSeattle),
        (ritaW)-[:ACTED_IN {roles: ['Suzy']}]->(sleeplessInSeattle),
        (billPull)-[:ACTED_IN {roles: ['Walter']}]->(sleeplessInSeattle),
        (victorG)-[:ACTED_IN {roles: ['Greg']}]->(sleeplessInSeattle),
        (rosieO)-[:ACTED_IN {roles: ['Becky']}]->(sleeplessInSeattle),
        (noraE)-[:DIRECTED]->(sleeplessInSeattle)

      CREATE (joeVersustheVolcano:Movie {title: 'Joe-Versus-the-Volcano', released: 1990,
          tagline: 'A story of love'})
      CREATE (johnS:Person {name: 'John Patrick Stanley', born: 1950})
      CREATE (nathan:Person {name: 'Nathan Lane', born: 1956})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Joe Banks']}]->(joeVersustheVolcano),
        (megR)-[:ACTED_IN {roles: ['DeDe', 'Angelica Graynamore', 'Patricia Graynamore']}]->(joeVersustheVolcano),
        (nathan)-[:ACTED_IN {roles: ['Baw']}]->(joeVersustheVolcano),
        (johnS)-[:DIRECTED]->(joeVersustheVolcano)

      CREATE (whenHarryMetSally:Movie {title: 'When-Harry-Met-Sally', released: 1998,
          tagline: 'When-Harry-Met-Sally'})
      CREATE (billyC:Person {name: 'Billy Crystal', born: 1948})
      CREATE (carrieF:Person {name: 'Carrie Fisher', born: 1956})
      CREATE (brunoK:Person {name: 'Bruno Kirby', born: 1949})
      CREATE
        (billyC)-[:ACTED_IN {roles: ['Harry Burns']}]->(whenHarryMetSally),
        (megR)-[:ACTED_IN {roles: ['Sally Albright']}]->(whenHarryMetSally),
        (carrieF)-[:ACTED_IN {roles: ['Marie']}]->(whenHarryMetSally),
        (brunoK)-[:ACTED_IN {roles: ['Jess']}]->(whenHarryMetSally),
        (robR)-[:DIRECTED]->(whenHarryMetSally),
        (robR)-[:PRODUCED]->(whenHarryMetSally),
        (noraE)-[:PRODUCED]->(whenHarryMetSally),
        (noraE)-[:WROTE]->(whenHarryMetSally)

      CREATE (thatThingYouDo:Movie {title: 'That-Thing-You-Do', released: 1996,
          tagline: 'There comes a time...'})
      CREATE (livT:Person {name: 'Liv Tyler', born: 1977})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Mr. White']}]->(thatThingYouDo),
        (livT)-[:ACTED_IN {roles: ['Faye Dolan']}]->(thatThingYouDo),
        (charlize)-[:ACTED_IN {roles: ['Tina']}]->(thatThingYouDo),
        (tomH)-[:DIRECTED]->(thatThingYouDo)

      CREATE (theReplacements:Movie {title: 'The Replacements', released: 2000,
          tagline: 'Pain heals, Chicks dig scars... Glory lasts forever'})
      CREATE (brooke:Person {name: 'Brooke Langton', born: 1970})
      CREATE (gene:Person {name: 'Gene Hackman', born: 1930})
      CREATE (orlando:Person {name: 'Orlando Jones', born: 1968})
      CREATE (howard:Person {name: 'Howard Deutch', born: 1950})
      CREATE
        (keanu)-[:ACTED_IN {roles: ['Shane Falco']}]->(theReplacements),
        (brooke)-[:ACTED_IN {roles: ['Annabelle Farrell']}]->(theReplacements),
        (gene)-[:ACTED_IN {roles: ['Jimmy McGinty']}]->(theReplacements),
        (orlando)-[:ACTED_IN {roles: ['Clifford Franklin']}]->(theReplacements),
        (howard)-[:DIRECTED]->(theReplacements)

      CREATE (rescueDawn:Movie {title: 'RescueDawn', released: 2006,
          tagline: 'The extraordinary true story'})
      CREATE (christianB:Person {name: 'Christian Bale', born: 1974})
      CREATE (zachG:Person {name: 'Zach Grenier', born: 1954})
      CREATE
        (marshallB)-[:ACTED_IN {roles: ['Admiral']}]->(rescueDawn),
        (christianB)-[:ACTED_IN {roles: ['Dieter Dengler']}]->(rescueDawn),
        (zachG)-[:ACTED_IN {roles: ['Squad Leader']}]->(rescueDawn),
        (steveZ)-[:ACTED_IN {roles: ['Duane']}]->(rescueDawn),
        (wernerH)-[:DIRECTED]->(rescueDawn)

      CREATE (theBirdcage:Movie {title: 'The-Birdcage', released: 1996, tagline: 'Come-as-you-are'})
      CREATE (mikeN:Person {name: 'Mike Nichols', born: 1931})
      CREATE
        (robin)-[:ACTED_IN {roles: ['Armand Goldman']}]->(theBirdcage),
        (nathan)-[:ACTED_IN {roles: ['Albert Goldman']}]->(theBirdcage),
        (gene)-[:ACTED_IN {roles: ['Sen. Kevin Keeley']}]->(theBirdcage),
        (mikeN)-[:DIRECTED]->(theBirdcage)

      CREATE (unforgiven:Movie {title: 'Unforgiven', released: 1992,
          tagline: 'It\'s a hell of a thing, killing a man'})
      CREATE (richardH:Person {name: 'Richard Harris', born: 1930})
      CREATE (clintE:Person {name: 'Clint Eastwood', born: 1930})
      CREATE
        (richardH)-[:ACTED_IN {roles: ['English Bob']}]->(unforgiven),
        (clintE)-[:ACTED_IN {roles: ['Bill Munny']}]->(unforgiven),
        (gene)-[:ACTED_IN {roles: ['Little Bill Daggett']}]->(unforgiven),
        (clintE)-[:DIRECTED]->(unforgiven)

      CREATE (johnnyMnemonic:Movie {title: 'Johnny-Mnemonic', released: 1995,
          tagline: 'The-hottest-data-in-the-coolest-head'})
      CREATE (takeshi:Person {name: 'Takeshi Kitano', born: 1947})
      CREATE (dina:Person {name: 'Dina Meyer', born: 1968})
      CREATE (iceT:Person {name: 'Ice-T', born: 1958})
      CREATE (robertL:Person {name: 'Robert Longo', born: 1953})
      CREATE
        (keanu)-[:ACTED_IN {roles: ['Johnny Mnemonic']}]->(johnnyMnemonic),
        (takeshi)-[:ACTED_IN {roles: ['Takahashi']}]->(johnnyMnemonic),
        (dina)-[:ACTED_IN {roles: ['Jane']}]->(johnnyMnemonic),
        (iceT)-[:ACTED_IN {roles: ['J-Bone']}]->(johnnyMnemonic),
        (robertL)-[:DIRECTED]->(johnnyMnemonic)

      CREATE (cloudAtlas:Movie {title: 'Cloud Atlas', released: 2012, tagline: 'Everything is connected'})
      CREATE (halleB:Person {name: 'Halle Berry', born: 1966})
      CREATE (jimB:Person {name: 'Jim Broadbent', born: 1949})
      CREATE (tomT:Person {name: 'Tom Tykwer', born: 1965})
      CREATE (davidMitchell:Person {name: 'David Mitchell', born: 1969})
      CREATE (stefanArndt:Person {name: 'Stefan Arndt', born: 1961})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Zachry', 'Dr. Henry Goose', 'Isaac Sachs', 'Dermot Hoggins']}]->(cloudAtlas),
        (hugo)-[:ACTED_IN {roles: ['Bill Smoke', 'Haskell Moore', 'Tadeusz Kesselring', 'Nurse Noakes', 'Boardman Mephi', 'Old Georgie']}]->(cloudAtlas),
        (halleB)-[:ACTED_IN {roles: ['Luisa Rey', 'Jocasta Ayrs', 'Ovid', 'Meronym']}]->(cloudAtlas),
        (jimB)-[:ACTED_IN {roles: ['Vyvyan Ayrs', 'Captain Molyneux', 'Timothy Cavendish']}]->(cloudAtlas),
        (tomT)-[:DIRECTED]->(cloudAtlas),
        (andyW)-[:DIRECTED]->(cloudAtlas),
        (lanaW)-[:DIRECTED]->(cloudAtlas),
        (davidMitchell)-[:WROTE]->(cloudAtlas),
        (stefanArndt)-[:PRODUCED]->(cloudAtlas)

      CREATE (theDaVinciCode:Movie {title: 'The Da Vinci Code', released: 2006, tagline: 'Break The Codes'})
      CREATE (ianM:Person {name: 'Ian McKellen', born: 1939})
      CREATE (audreyT:Person {name: 'Audrey Tautou', born: 1976})
      CREATE (paulB:Person {name: 'Paul Bettany', born: 1971})
      CREATE (ronH:Person {name: 'Ron Howard', born: 1954})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Dr. Robert Langdon']}]->(theDaVinciCode),
        (ianM)-[:ACTED_IN {roles: ['Sir Leight Teabing']}]->(theDaVinciCode),
        (audreyT)-[:ACTED_IN {roles: ['Sophie Neveu']}]->(theDaVinciCode),
        (paulB)-[:ACTED_IN {roles: ['Silas']}]->(theDaVinciCode),
        (ronH)-[:DIRECTED]->(theDaVinciCode)

      CREATE (vforVendetta:Movie {title: 'V for Vendetta', released: 2006, tagline: 'Freedom! Forever!'})
      CREATE (natalieP:Person {name: 'Natalie Portman', born: 1981})
      CREATE (stephenR:Person {name: 'Stephen Rea', born: 1946})
      CREATE (johnH:Person {name: 'John Hurt', born: 1940})
      CREATE (benM:Person {name: 'Ben Miles', born: 1967})
      CREATE
        (hugo)-[:ACTED_IN {roles: ['V']}]->(vforVendetta),
        (natalieP)-[:ACTED_IN {roles: ['Evey Hammond']}]->(vforVendetta),
        (stephenR)-[:ACTED_IN {roles: ['Eric Finch']}]->(vforVendetta),
        (johnH)-[:ACTED_IN {roles: ['High Chancellor Adam Sutler']}]->(vforVendetta),
        (benM)-[:ACTED_IN {roles: ['Dascomb']}]->(vforVendetta),
        (jamesM)-[:DIRECTED]->(vforVendetta),
        (andyW)-[:PRODUCED]->(vforVendetta),
        (lanaW)-[:PRODUCED]->(vforVendetta),
        (joelS)-[:PRODUCED]->(vforVendetta),
        (andyW)-[:WROTE]->(vforVendetta),
        (lanaW)-[:WROTE]->(vforVendetta)

      CREATE (speedRacer:Movie {title: 'Speed Racer', released: 2008, tagline: 'Speed has no limits'})
      CREATE (emileH:Person {name: 'Emile Hirsch', born: 1985})
      CREATE (johnG:Person {name: 'John Goodman', born: 1960})
      CREATE (susanS:Person {name: 'Susan Sarandon', born: 1946})
      CREATE (matthewF:Person {name: 'Matthew Fox', born: 1966})
      CREATE (christinaR:Person {name: 'Christina Ricci', born: 1980})
      CREATE (rain:Person {name: 'Rain', born: 1982})
      CREATE
        (emileH)-[:ACTED_IN {roles: ['Speed Racer']}]->(speedRacer),
        (johnG)-[:ACTED_IN {roles: ['Pops']}]->(speedRacer),
        (susanS)-[:ACTED_IN {roles: ['Mom']}]->(speedRacer),
        (matthewF)-[:ACTED_IN {roles: ['Racer X']}]->(speedRacer),
        (christinaR)-[:ACTED_IN {roles: ['Trixie']}]->(speedRacer),
        (rain)-[:ACTED_IN {roles: ['Taejo Togokahn']}]->(speedRacer),
        (benM)-[:ACTED_IN {roles: ['Cass Jones']}]->(speedRacer),
        (andyW)-[:DIRECTED]->(speedRacer),
        (lanaW)-[:DIRECTED]->(speedRacer),
        (andyW)-[:WROTE]->(speedRacer),
        (lanaW)-[:WROTE]->(speedRacer),
        (joelS)-[:PRODUCED]->(speedRacer)

      CREATE (ninjaAssassin:Movie {title: 'Ninja Assassin', released: 2009,
          tagline: 'Prepare to enter a secret world of assassins'})
      CREATE (naomieH:Person {name: 'Naomie Harris'})
      CREATE
        (rain)-[:ACTED_IN {roles: ['Raizo']}]->(ninjaAssassin),
        (naomieH)-[:ACTED_IN {roles: ['Mika Coretti']}]->(ninjaAssassin),
        (rickY)-[:ACTED_IN {roles: ['Takeshi']}]->(ninjaAssassin),
        (benM)-[:ACTED_IN {roles: ['Ryan Maslow']}]->(ninjaAssassin),
        (jamesM)-[:DIRECTED]->(ninjaAssassin),
        (andyW)-[:PRODUCED]->(ninjaAssassin),
        (lanaW)-[:PRODUCED]->(ninjaAssassin),
        (joelS)-[:PRODUCED]->(ninjaAssassin)

      CREATE (theGreenMile:Movie {title: 'The Green Mile', released: 1999,
          tagline: 'Walk a mile you\'ll never forget.'})
      CREATE (michaelD:Person {name: 'Michael Clarke Duncan', born: 1957})
      CREATE (davidM:Person {name: 'David Morse', born: 1953})
      CREATE (samR:Person {name: 'Sam Rockwell', born: 1968})
      CREATE (garyS:Person {name: 'Gary Sinise', born: 1955})
      CREATE (patriciaC:Person {name: 'Patricia Clarkson', born: 1959})
      CREATE (frankD:Person {name: 'Frank Darabont', born: 1959})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Paul Edgecomb']}]->(theGreenMile),
        (michaelD)-[:ACTED_IN {roles: ['John Coffey']}]->(theGreenMile),
        (davidM)-[:ACTED_IN {roles: ['Brutus Brutal Howell']}]->(theGreenMile),
        (bonnieH)-[:ACTED_IN {roles: ['Jan Edgecomb']}]->(theGreenMile),
        (jamesC)-[:ACTED_IN {roles: ['Warden Hal Moores']}]->(theGreenMile),
        (samR)-[:ACTED_IN {roles: ['Wild Bill Wharton']}]->(theGreenMile),
        (garyS)-[:ACTED_IN {roles: ['Burt Hammersmith']}]->(theGreenMile),
        (patriciaC)-[:ACTED_IN {roles: ['Melinda Moores']}]->(theGreenMile),
        (frankD)-[:DIRECTED]->(theGreenMile)

      CREATE (frostNixon:Movie {title: 'Frost/Nixon', released: 2008,
          tagline: '400 million people were waiting for the truth.'})
      CREATE (frankL:Person {name: 'Frank Langella', born: 1938})
      CREATE (michaelS:Person {name: 'Michael Sheen', born: 1969})
      CREATE (oliverP:Person {name: 'Oliver Platt', born: 1960})
      CREATE
        (frankL)-[:ACTED_IN {roles: ['Richard Nixon']}]->(frostNixon),
        (michaelS)-[:ACTED_IN {roles: ['David Frost']}]->(frostNixon),
        (kevinB)-[:ACTED_IN {roles: ['Jack Brennan']}]->(frostNixon),
        (oliverP)-[:ACTED_IN {roles: ['Bob Zelnick']}]->(frostNixon),
        (samR)-[:ACTED_IN {roles: ['James Reston, Jr.']}]->(frostNixon),
        (ronH)-[:DIRECTED]->(frostNixon)

      CREATE (hoffa:Movie {title: 'Hoffa', released: 1992, tagline: "He didn't want law. He wanted justice."})
      CREATE (dannyD:Person {name: 'Danny DeVito', born: 1944})
      CREATE (johnR:Person {name: 'John C. Reilly', born: 1965})
      CREATE
        (jackN)-[:ACTED_IN {roles: ['Hoffa']}]->(hoffa),
        (dannyD)-[:ACTED_IN {roles: ['Robert Bobby Ciaro']}]->(hoffa),
        (jTW)-[:ACTED_IN {roles: ['Frank Fitzsimmons']}]->(hoffa),
        (johnR)-[:ACTED_IN {roles: ['Peter Connelly']}]->(hoffa),
        (dannyD)-[:DIRECTED]->(hoffa)

      CREATE (apollo13:Movie {title: 'Apollo 13', released: 1995, tagline: 'Houston, we have a problem.'})
      CREATE (edH:Person {name: 'Ed Harris', born: 1950})
      CREATE (billPax:Person {name: 'Bill Paxton', born: 1955})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Jim Lovell']}]->(apollo13),
        (kevinB)-[:ACTED_IN {roles: ['Jack Swigert']}]->(apollo13),
        (edH)-[:ACTED_IN {roles: ['Gene Kranz']}]->(apollo13),
        (billPax)-[:ACTED_IN {roles: ['Fred Haise']}]->(apollo13),
        (garyS)-[:ACTED_IN {roles: ['Ken Mattingly']}]->(apollo13),
        (ronH)-[:DIRECTED]->(apollo13)

      CREATE (twister:Movie {title: 'Twister', released: 1996, tagline: 'Don\'t Breathe. Don\'t Look Back.'})
      CREATE (philipH:Person {name: 'Philip Seymour Hoffman', born: 1967})
      CREATE (janB:Person {name: 'Jan de Bont', born: 1943})
      CREATE
        (billPax)-[:ACTED_IN {roles: ['Bill Harding']}]->(twister),
        (helenH)-[:ACTED_IN {roles: ['Dr. Jo Harding']}]->(twister),
        (zachG)-[:ACTED_IN {roles: ['Eddie']}]->(twister),
        (philipH)-[:ACTED_IN {roles: ['Dustin Davis']}]->(twister),
        (janB)-[:DIRECTED]->(twister)

      CREATE (castAway:Movie {title: 'Cast Away', released: 2000,
          tagline: 'At the edge of the world, his journey begins.'})
      CREATE (robertZ:Person {name: 'Robert Zemeckis', born: 1951})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Chuck Noland']}]->(castAway),
        (helenH)-[:ACTED_IN {roles: ['Kelly Frears']}]->(castAway),
        (robertZ)-[:DIRECTED]->(castAway)

      CREATE (oneFlewOvertheCuckoosNest:Movie {title: 'One Flew Over the Cuckoo\'s Nest', released: 1975,
          tagline: 'If he is crazy, what does that make you?'})
      CREATE (milosF:Person {name: 'Milos Forman', born: 1932})
      CREATE
        (jackN)-[:ACTED_IN {roles: ['Randle McMurphy']}]->(oneFlewOvertheCuckoosNest),
        (dannyD)-[:ACTED_IN {roles: ['Martini']}]->(oneFlewOvertheCuckoosNest),
        (milosF)-[:DIRECTED]->(oneFlewOvertheCuckoosNest)

      CREATE (somethingsGottaGive:Movie {title: 'Something\'s Gotta Give', released: 2003})
      CREATE (dianeK:Person {name: 'Diane Keaton', born: 1946})
      CREATE (nancyM:Person {name: 'Nancy Meyers', born: 1949})
      CREATE
        (jackN)-[:ACTED_IN {roles: ['Harry Sanborn']}]->(somethingsGottaGive),
        (dianeK)-[:ACTED_IN {roles: ['Erica Barry']}]->(somethingsGottaGive),
        (keanu)-[:ACTED_IN {roles: ['Julian Mercer']}]->(somethingsGottaGive),
        (nancyM)-[:DIRECTED]->(somethingsGottaGive),
        (nancyM)-[:PRODUCED]->(somethingsGottaGive),
        (nancyM)-[:WROTE]->(somethingsGottaGive)

      CREATE (bicentennialMan:Movie {title: 'Bicentennial Man', released: 1999,
          tagline: 'One robot\'s 200 year journey to become an ordinary man.'})
      CREATE (chrisC:Person {name: 'Chris Columbus', born: 1958})
      CREATE
        (robin)-[:ACTED_IN {roles: ['Andrew Marin']}]->(bicentennialMan),
        (oliverP)-[:ACTED_IN {roles: ['Rupert Burns']}]->(bicentennialMan),
        (chrisC)-[:DIRECTED]->(bicentennialMan)

      CREATE (charlieWilsonsWar:Movie {title: 'Charlie Wilson\'s War', released: 2007,
          tagline: 'A stiff drink. A little mascara. A lot of nerve. Who said they could not bring down the Soviet empire.'})
      CREATE (juliaR:Person {name: 'Julia Roberts', born: 1967})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Rep. Charlie Wilson']}]->(charlieWilsonsWar),
        (juliaR)-[:ACTED_IN {roles: ['Joanne Herring']}]->(charlieWilsonsWar),
        (philipH)-[:ACTED_IN {roles: ['Gust Avrakotos']}]->(charlieWilsonsWar),
        (mikeN)-[:DIRECTED]->(charlieWilsonsWar)

      CREATE (thePolarExpress:Movie {title: 'The Polar Express', released: 2004,
          tagline: 'This Holiday Season... Believe'})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Hero Boy', 'Father', 'Conductor', 'Hobo', 'Scrooge', 'Santa Claus']}]->(thePolarExpress),
        (robertZ)-[:DIRECTED]->(thePolarExpress)

      CREATE (aLeagueofTheirOwn:Movie {title: 'A League of Their Own', released: 1992,
          tagline: 'A league of their own'})
      CREATE (madonna:Person {name: 'Madonna', born: 1954})
      CREATE (geenaD:Person {name: 'Geena Davis', born: 1956})
      CREATE (loriP:Person {name: 'Lori Petty', born: 1963})
      CREATE (pennyM:Person {name: 'Penny Marshall', born: 1943})
      CREATE
        (tomH)-[:ACTED_IN {roles: ['Jimmy Dugan']}]->(aLeagueofTheirOwn),
        (geenaD)-[:ACTED_IN {roles: ['Dottie Hinson']}]->(aLeagueofTheirOwn),
        (loriP)-[:ACTED_IN {roles: ['Kit Keller']}]->(aLeagueofTheirOwn),
        (rosieO)-[:ACTED_IN {roles: ['Doris Murphy']}]->(aLeagueofTheirOwn),
        (madonna)-[:ACTED_IN {roles: ['Mae Mordabito']}]->(aLeagueofTheirOwn),
        (billPax)-[:ACTED_IN {roles: ['Bob Hinson']}]->(aLeagueofTheirOwn),
        (pennyM)-[:DIRECTED]->(aLeagueofTheirOwn)

      CREATE (paulBlythe:Person {name: 'Paul Blythe'})
      CREATE (angelaScope:Person {name: 'Angela Scope'})
      CREATE (jessicaThompson:Person {name: 'Jessica Thompson'})
      CREATE (jamesThompson:Person {name: 'James Thompson'})

      CREATE
        (jamesThompson)-[:FOLLOWS]->(jessicaThompson),
        (angelaScope)-[:FOLLOWS]->(jessicaThompson),
        (paulBlythe)-[:FOLLOWS]->(angelaScope)

      CREATE
        (jessicaThompson)-[:REVIEWED {summary: 'An amazing journey', rating: 95}]->(cloudAtlas),
        (jessicaThompson)-[:REVIEWED {summary: 'Silly, but fun', rating: 65}]->(theReplacements),
        (jamesThompson)-[:REVIEWED {summary: 'The coolest football movie ever', rating: 100}]->(theReplacements),
        (angelaScope)-[:REVIEWED {summary: 'Pretty funny at times', rating: 62}]->(theReplacements),
        (jessicaThompson)-[:REVIEWED {summary: 'Dark, but compelling', rating: 85}]->(unforgiven),
        (jessicaThompson)-[:REVIEWED {summary: 'Slapstick', rating: 45}]->(theBirdcage),
        (jessicaThompson)-[:REVIEWED {summary: 'A solid romp', rating: 68}]->(theDaVinciCode),
        (jamesThompson)-[:REVIEWED {summary: 'Fun, but a little far fetched', rating: 65}]->(theDaVinciCode),
        (jessicaThompson)-[:REVIEWED {summary: 'You had me at Jerry', rating: 92}]->(jerryMaguire)

      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 171 |
      | +relationships | 253 |
      | +properties    | 564 |
      | +labels        | 2   |

  Scenario: [2] Many CREATE clauses
    Given an empty graph
    When executing query:
      """
      CREATE (hf:School {name: 'Hilly Fields Technical College'})
      CREATE (hf)-[:STAFF]->(mrb:Teacher {name: 'Mr Balls'})
      CREATE (hf)-[:STAFF]->(mrspb:Teacher {name: 'Ms Packard-Bell'})
      CREATE (hf)-[:STAFF]->(mrs:Teacher {name: 'Mr Smith'})
      CREATE (hf)-[:STAFF]->(mrsa:Teacher {name: 'Mrs Adenough'})
      CREATE (hf)-[:STAFF]->(mrvdg:Teacher {name: 'Mr Van der Graaf'})
      CREATE (hf)-[:STAFF]->(msn:Teacher {name: 'Ms Noethe'})
      CREATE (hf)-[:STAFF]->(mrsn:Teacher {name: 'Mrs Noakes'})
      CREATE (hf)-[:STAFF]->(mrm:Teacher {name: 'Mr Marker'})
      CREATE (hf)-[:STAFF]->(msd:Teacher {name: 'Ms Delgado'})
      CREATE (hf)-[:STAFF]->(mrsg:Teacher {name: 'Mrs Glass'})
      CREATE (hf)-[:STAFF]->(mrf:Teacher {name: 'Mr Flint'})
      CREATE (hf)-[:STAFF]->(mrk:Teacher {name: 'Mr Kearney'})
      CREATE (hf)-[:STAFF]->(msf:Teacher {name: 'Mrs Forrester'})
      CREATE (hf)-[:STAFF]->(mrsf:Teacher {name: 'Mrs Fischer'})
      CREATE (hf)-[:STAFF]->(mrj:Teacher {name: 'Mr Jameson'})

      CREATE (hf)-[:STUDENT]->(_001:Student {name: 'Portia Vasquez'})
      CREATE (hf)-[:STUDENT]->(_002:Student {name: 'Andrew Parks'})
      CREATE (hf)-[:STUDENT]->(_003:Student {name: 'Germane Frye'})
      CREATE (hf)-[:STUDENT]->(_004:Student {name: 'Yuli Gutierrez'})
      CREATE (hf)-[:STUDENT]->(_005:Student {name: 'Kamal Solomon'})
      CREATE (hf)-[:STUDENT]->(_006:Student {name: 'Lysandra Porter'})
      CREATE (hf)-[:STUDENT]->(_007:Student {name: 'Stella Santiago'})
      CREATE (hf)-[:STUDENT]->(_008:Student {name: 'Brenda Torres'})
      CREATE (hf)-[:STUDENT]->(_009:Student {name: 'Heidi Dunlap'})

      CREATE (hf)-[:STUDENT]->(_010:Student {name: 'Halee Taylor'})
      CREATE (hf)-[:STUDENT]->(_011:Student {name: 'Brennan Crosby'})
      CREATE (hf)-[:STUDENT]->(_012:Student {name: 'Rooney Cook'})
      CREATE (hf)-[:STUDENT]->(_013:Student {name: 'Xavier Morrison'})
      CREATE (hf)-[:STUDENT]->(_014:Student {name: 'Zelenia Santana'})
      CREATE (hf)-[:STUDENT]->(_015:Student {name: 'Eaton Bonner'})
      CREATE (hf)-[:STUDENT]->(_016:Student {name: 'Leilani Bishop'})
      CREATE (hf)-[:STUDENT]->(_017:Student {name: 'Jamalia Pickett'})
      CREATE (hf)-[:STUDENT]->(_018:Student {name: 'Wynter Russell'})
      CREATE (hf)-[:STUDENT]->(_019:Student {name: 'Liberty Melton'})

      CREATE (hf)-[:STUDENT]->(_020:Student {name: 'MacKensie Obrien'})
      CREATE (hf)-[:STUDENT]->(_021:Student {name: 'Oprah Maynard'})
      CREATE (hf)-[:STUDENT]->(_022:Student {name: 'Lyle Parks'})
      CREATE (hf)-[:STUDENT]->(_023:Student {name: 'Madonna Justice'})
      CREATE (hf)-[:STUDENT]->(_024:Student {name: 'Herman Frederick'})
      CREATE (hf)-[:STUDENT]->(_025:Student {name: 'Preston Stevenson'})
      CREATE (hf)-[:STUDENT]->(_026:Student {name: 'Drew Carrillo'})
      CREATE (hf)-[:STUDENT]->(_027:Student {name: 'Hamilton Woodward'})
      CREATE (hf)-[:STUDENT]->(_028:Student {name: 'Buckminster Bradley'})
      CREATE (hf)-[:STUDENT]->(_029:Student {name: 'Shea Cote'})

      CREATE (hf)-[:STUDENT]->(_030:Student {name: 'Raymond Leonard'})
      CREATE (hf)-[:STUDENT]->(_031:Student {name: 'Gavin Branch'})
      CREATE (hf)-[:STUDENT]->(_032:Student {name: 'Kylan Powers'})
      CREATE (hf)-[:STUDENT]->(_033:Student {name: 'Hedy Bowers'})
      CREATE (hf)-[:STUDENT]->(_034:Student {name: 'Derek Church'})
      CREATE (hf)-[:STUDENT]->(_035:Student {name: 'Silas Santiago'})
      CREATE (hf)-[:STUDENT]->(_036:Student {name: 'Elton Bright'})
      CREATE (hf)-[:STUDENT]->(_037:Student {name: 'Dora Schmidt'})
      CREATE (hf)-[:STUDENT]->(_038:Student {name: 'Julian Sullivan'})
      CREATE (hf)-[:STUDENT]->(_039:Student {name: 'Willow Morton'})

      CREATE (hf)-[:STUDENT]->(_040:Student {name: 'Blaze Hines'})
      CREATE (hf)-[:STUDENT]->(_041:Student {name: 'Felicia Tillman'})
      CREATE (hf)-[:STUDENT]->(_042:Student {name: 'Ralph Webb'})
      CREATE (hf)-[:STUDENT]->(_043:Student {name: 'Roth Gilmore'})
      CREATE (hf)-[:STUDENT]->(_044:Student {name: 'Dorothy Burgess'})
      CREATE (hf)-[:STUDENT]->(_045:Student {name: 'Lana Sandoval'})
      CREATE (hf)-[:STUDENT]->(_046:Student {name: 'Nevada Strickland'})
      CREATE (hf)-[:STUDENT]->(_047:Student {name: 'Lucian Franco'})
      CREATE (hf)-[:STUDENT]->(_048:Student {name: 'Jasper Talley'})
      CREATE (hf)-[:STUDENT]->(_049:Student {name: 'Madaline Spears'})

      CREATE (hf)-[:STUDENT]->(_050:Student {name: 'Upton Browning'})
      CREATE (hf)-[:STUDENT]->(_051:Student {name: 'Cooper Leon'})
      CREATE (hf)-[:STUDENT]->(_052:Student {name: 'Celeste Ortega'})
      CREATE (hf)-[:STUDENT]->(_053:Student {name: 'Willa Hewitt'})
      CREATE (hf)-[:STUDENT]->(_054:Student {name: 'Rooney Bryan'})
      CREATE (hf)-[:STUDENT]->(_055:Student {name: 'Nayda Hays'})
      CREATE (hf)-[:STUDENT]->(_056:Student {name: 'Kadeem Salazar'})
      CREATE (hf)-[:STUDENT]->(_057:Student {name: 'Halee Allen'})
      CREATE (hf)-[:STUDENT]->(_058:Student {name: 'Odysseus Mayo'})
      CREATE (hf)-[:STUDENT]->(_059:Student {name: 'Kato Merrill'})

      CREATE (hf)-[:STUDENT]->(_060:Student {name: 'Halee Juarez'})
      CREATE (hf)-[:STUDENT]->(_061:Student {name: 'Chloe Charles'})
      CREATE (hf)-[:STUDENT]->(_062:Student {name: 'Abel Montoya'})
      CREATE (hf)-[:STUDENT]->(_063:Student {name: 'Hilda Welch'})
      CREATE (hf)-[:STUDENT]->(_064:Student {name: 'Britanni Bean'})
      CREATE (hf)-[:STUDENT]->(_065:Student {name: 'Joelle Beach'})
      CREATE (hf)-[:STUDENT]->(_066:Student {name: 'Ciara Odom'})
      CREATE (hf)-[:STUDENT]->(_067:Student {name: 'Zia Williams'})
      CREATE (hf)-[:STUDENT]->(_068:Student {name: 'Darrel Bailey'})
      CREATE (hf)-[:STUDENT]->(_069:Student {name: 'Lance Mcdowell'})

      CREATE (hf)-[:STUDENT]->(_070:Student {name: 'Clayton Bullock'})
      CREATE (hf)-[:STUDENT]->(_071:Student {name: 'Roanna Mosley'})
      CREATE (hf)-[:STUDENT]->(_072:Student {name: 'Amethyst Mcclure'})
      CREATE (hf)-[:STUDENT]->(_073:Student {name: 'Hanae Mann'})
      CREATE (hf)-[:STUDENT]->(_074:Student {name: 'Graiden Haynes'})
      CREATE (hf)-[:STUDENT]->(_075:Student {name: 'Marcia Byrd'})
      CREATE (hf)-[:STUDENT]->(_076:Student {name: 'Yoshi Joyce'})
      CREATE (hf)-[:STUDENT]->(_077:Student {name: 'Gregory Sexton'})
      CREATE (hf)-[:STUDENT]->(_078:Student {name: 'Nash Carey'})
      CREATE (hf)-[:STUDENT]->(_079:Student {name: 'Rae Stevens'})

      CREATE (hf)-[:STUDENT]->(_080:Student {name: 'Blossom Fulton'})
      CREATE (hf)-[:STUDENT]->(_081:Student {name: 'Lev Curry'})
      CREATE (hf)-[:STUDENT]->(_082:Student {name: 'Margaret Gamble'})
      CREATE (hf)-[:STUDENT]->(_083:Student {name: 'Rylee Patterson'})
      CREATE (hf)-[:STUDENT]->(_084:Student {name: 'Harper Perkins'})
      CREATE (hf)-[:STUDENT]->(_085:Student {name: 'Kennan Murphy'})
      CREATE (hf)-[:STUDENT]->(_086:Student {name: 'Hilda Coffey'})
      CREATE (hf)-[:STUDENT]->(_087:Student {name: 'Marah Reed'})
      CREATE (hf)-[:STUDENT]->(_088:Student {name: 'Blaine Wade'})
      CREATE (hf)-[:STUDENT]->(_089:Student {name: 'Geraldine Sanders'})

      CREATE (hf)-[:STUDENT]->(_090:Student {name: 'Kerry Rollins'})
      CREATE (hf)-[:STUDENT]->(_091:Student {name: 'Virginia Sweet'})
      CREATE (hf)-[:STUDENT]->(_092:Student {name: 'Sophia Merrill'})
      CREATE (hf)-[:STUDENT]->(_093:Student {name: 'Hedda Carson'})
      CREATE (hf)-[:STUDENT]->(_094:Student {name: 'Tamekah Charles'})
      CREATE (hf)-[:STUDENT]->(_095:Student {name: 'Knox Barton'})
      CREATE (hf)-[:STUDENT]->(_096:Student {name: 'Ariel Porter'})
      CREATE (hf)-[:STUDENT]->(_097:Student {name: 'Berk Wooten'})
      CREATE (hf)-[:STUDENT]->(_098:Student {name: 'Galena Glenn'})
      CREATE (hf)-[:STUDENT]->(_099:Student {name: 'Jolene Anderson'})

      CREATE (hf)-[:STUDENT]->(_100:Student {name: 'Leonard Hewitt'})
      CREATE (hf)-[:STUDENT]->(_101:Student {name: 'Maris Salazar'})
      CREATE (hf)-[:STUDENT]->(_102:Student {name: 'Brian Frost'})
      CREATE (hf)-[:STUDENT]->(_103:Student {name: 'Zane Moses'})
      CREATE (hf)-[:STUDENT]->(_104:Student {name: 'Serina Finch'})
      CREATE (hf)-[:STUDENT]->(_105:Student {name: 'Anastasia Fletcher'})
      CREATE (hf)-[:STUDENT]->(_106:Student {name: 'Glenna Chapman'})
      CREATE (hf)-[:STUDENT]->(_107:Student {name: 'Mufutau Gillespie'})
      CREATE (hf)-[:STUDENT]->(_108:Student {name: 'Basil Guthrie'})
      CREATE (hf)-[:STUDENT]->(_109:Student {name: 'Theodore Marsh'})

      CREATE (hf)-[:STUDENT]->(_110:Student {name: 'Jaime Contreras'})
      CREATE (hf)-[:STUDENT]->(_111:Student {name: 'Irma Poole'})
      CREATE (hf)-[:STUDENT]->(_112:Student {name: 'Buckminster Bender'})
      CREATE (hf)-[:STUDENT]->(_113:Student {name: 'Elton Morris'})
      CREATE (hf)-[:STUDENT]->(_114:Student {name: 'Barbara Nguyen'})
      CREATE (hf)-[:STUDENT]->(_115:Student {name: 'Tanya Kidd'})
      CREATE (hf)-[:STUDENT]->(_116:Student {name: 'Kaden Hoover'})
      CREATE (hf)-[:STUDENT]->(_117:Student {name: 'Christopher Bean'})
      CREATE (hf)-[:STUDENT]->(_118:Student {name: 'Trevor Daugherty'})
      CREATE (hf)-[:STUDENT]->(_119:Student {name: 'Rudyard Bates'})

      CREATE (hf)-[:STUDENT]->(_120:Student {name: 'Stacy Monroe'})
      CREATE (hf)-[:STUDENT]->(_121:Student {name: 'Kieran Keller'})
      CREATE (hf)-[:STUDENT]->(_122:Student {name: 'Ivy Garrison'})
      CREATE (hf)-[:STUDENT]->(_123:Student {name: 'Miranda Haynes'})
      CREATE (hf)-[:STUDENT]->(_124:Student {name: 'Abigail Heath'})
      CREATE (hf)-[:STUDENT]->(_125:Student {name: 'Margaret Santiago'})
      CREATE (hf)-[:STUDENT]->(_126:Student {name: 'Cade Floyd'})
      CREATE (hf)-[:STUDENT]->(_127:Student {name: 'Allen Crane'})
      CREATE (hf)-[:STUDENT]->(_128:Student {name: 'Stella Gilliam'})
      CREATE (hf)-[:STUDENT]->(_129:Student {name: 'Rashad Miller'})

      CREATE (hf)-[:STUDENT]->(_130:Student {name: 'Francis Cox'})
      CREATE (hf)-[:STUDENT]->(_131:Student {name: 'Darryl Rosario'})
      CREATE (hf)-[:STUDENT]->(_132:Student {name: 'Michael Daniels'})
      CREATE (hf)-[:STUDENT]->(_133:Student {name: 'Aretha Henderson'})
      CREATE (hf)-[:STUDENT]->(_134:Student {name: 'Roth Barrera'})
      CREATE (hf)-[:STUDENT]->(_135:Student {name: 'Yael Day'})
      CREATE (hf)-[:STUDENT]->(_136:Student {name: 'Wynter Richmond'})
      CREATE (hf)-[:STUDENT]->(_137:Student {name: 'Quyn Flowers'})
      CREATE (hf)-[:STUDENT]->(_138:Student {name: 'Yvette Marquez'})
      CREATE (hf)-[:STUDENT]->(_139:Student {name: 'Teagan Curry'})

      CREATE (hf)-[:STUDENT]->(_140:Student {name: 'Brenden Bishop'})
      CREATE (hf)-[:STUDENT]->(_141:Student {name: 'Montana Black'})
      CREATE (hf)-[:STUDENT]->(_142:Student {name: 'Ramona Parker'})
      CREATE (hf)-[:STUDENT]->(_143:Student {name: 'Merritt Hansen'})
      CREATE (hf)-[:STUDENT]->(_144:Student {name: 'Melvin Vang'})
      CREATE (hf)-[:STUDENT]->(_145:Student {name: 'Samantha Perez'})
      CREATE (hf)-[:STUDENT]->(_146:Student {name: 'Thane Porter'})
      CREATE (hf)-[:STUDENT]->(_147:Student {name: 'Vaughan Haynes'})
      CREATE (hf)-[:STUDENT]->(_148:Student {name: 'Irma Miles'})
      CREATE (hf)-[:STUDENT]->(_149:Student {name: 'Amery Jensen'})

      CREATE (hf)-[:STUDENT]->(_150:Student {name: 'Montana Holman'})
      CREATE (hf)-[:STUDENT]->(_151:Student {name: 'Kimberly Langley'})
      CREATE (hf)-[:STUDENT]->(_152:Student {name: 'Ebony Bray'})
      CREATE (hf)-[:STUDENT]->(_153:Student {name: 'Ishmael Pollard'})
      CREATE (hf)-[:STUDENT]->(_154:Student {name: 'Illana Thompson'})
      CREATE (hf)-[:STUDENT]->(_155:Student {name: 'Rhona Bowers'})
      CREATE (hf)-[:STUDENT]->(_156:Student {name: 'Lilah Dotson'})
      CREATE (hf)-[:STUDENT]->(_157:Student {name: 'Shelly Roach'})
      CREATE (hf)-[:STUDENT]->(_158:Student {name: 'Celeste Woodward'})
      CREATE (hf)-[:STUDENT]->(_159:Student {name: 'Christen Lynn'})

      CREATE (hf)-[:STUDENT]->(_160:Student {name: 'Miranda Slater'})
      CREATE (hf)-[:STUDENT]->(_161:Student {name: 'Lunea Clements'})
      CREATE (hf)-[:STUDENT]->(_162:Student {name: 'Lester Francis'})
      CREATE (hf)-[:STUDENT]->(_163:Student {name: 'David Fischer'})
      CREATE (hf)-[:STUDENT]->(_164:Student {name: 'Kyra Bean'})
      CREATE (hf)-[:STUDENT]->(_165:Student {name: 'Imelda Alston'})
      CREATE (hf)-[:STUDENT]->(_166:Student {name: 'Finn Farrell'})
      CREATE (hf)-[:STUDENT]->(_167:Student {name: 'Kirby House'})
      CREATE (hf)-[:STUDENT]->(_168:Student {name: 'Amanda Zamora'})
      CREATE (hf)-[:STUDENT]->(_169:Student {name: 'Rina Franco'})

      CREATE (hf)-[:STUDENT]->(_170:Student {name: 'Sonia Lane'})
      CREATE (hf)-[:STUDENT]->(_171:Student {name: 'Nora Jefferson'})
      CREATE (hf)-[:STUDENT]->(_172:Student {name: 'Colton Ortiz'})
      CREATE (hf)-[:STUDENT]->(_173:Student {name: 'Alden Munoz'})
      CREATE (hf)-[:STUDENT]->(_174:Student {name: 'Ferdinand Cline'})
      CREATE (hf)-[:STUDENT]->(_175:Student {name: 'Cynthia Prince'})
      CREATE (hf)-[:STUDENT]->(_176:Student {name: 'Asher Hurst'})
      CREATE (hf)-[:STUDENT]->(_177:Student {name: 'MacKensie Stevenson'})
      CREATE (hf)-[:STUDENT]->(_178:Student {name: 'Sydnee Sosa'})
      CREATE (hf)-[:STUDENT]->(_179:Student {name: 'Dante Callahan'})

      CREATE (hf)-[:STUDENT]->(_180:Student {name: 'Isabella Santana'})
      CREATE (hf)-[:STUDENT]->(_181:Student {name: 'Raven Bowman'})
      CREATE (hf)-[:STUDENT]->(_182:Student {name: 'Kirby Bolton'})
      CREATE (hf)-[:STUDENT]->(_183:Student {name: 'Peter Shaffer'})
      CREATE (hf)-[:STUDENT]->(_184:Student {name: 'Fletcher Beard'})
      CREATE (hf)-[:STUDENT]->(_185:Student {name: 'Irene Lowe'})
      CREATE (hf)-[:STUDENT]->(_186:Student {name: 'Ella Talley'})
      CREATE (hf)-[:STUDENT]->(_187:Student {name: 'Jorden Kerr'})
      CREATE (hf)-[:STUDENT]->(_188:Student {name: 'Macey Delgado'})
      CREATE (hf)-[:STUDENT]->(_189:Student {name: 'Ulysses Graves'})

      CREATE (hf)-[:STUDENT]->(_190:Student {name: 'Declan Blake'})
      CREATE (hf)-[:STUDENT]->(_191:Student {name: 'Lila Hurst'})
      CREATE (hf)-[:STUDENT]->(_192:Student {name: 'David Rasmussen'})
      CREATE (hf)-[:STUDENT]->(_193:Student {name: 'Desiree Cortez'})
      CREATE (hf)-[:STUDENT]->(_194:Student {name: 'Myles Horton'})
      CREATE (hf)-[:STUDENT]->(_195:Student {name: 'Rylee Willis'})
      CREATE (hf)-[:STUDENT]->(_196:Student {name: 'Kelsey Yates'})
      CREATE (hf)-[:STUDENT]->(_197:Student {name: 'Alika Stanton'})
      CREATE (hf)-[:STUDENT]->(_198:Student {name: 'Ria Campos'})
      CREATE (hf)-[:STUDENT]->(_199:Student {name: 'Elijah Hendricks'})

      CREATE (hf)-[:STUDENT]->(_200:Student {name: 'Hayes House'})

      CREATE (hf)-[:DEPARTMENT]->(md:Department {name: 'Mathematics'})
      CREATE (hf)-[:DEPARTMENT]->(sd:Department {name: 'Science'})
      CREATE (hf)-[:DEPARTMENT]->(ed:Department {name: 'Engineering'})

      CREATE (pm:Subject {name: 'Pure Mathematics'})
      CREATE (am:Subject {name: 'Applied Mathematics'})
      CREATE (ph:Subject {name: 'Physics'})
      CREATE (ch:Subject {name: 'Chemistry'})
      CREATE (bi:Subject {name: 'Biology'})
      CREATE (es:Subject {name: 'Earth Science'})
      CREATE (me:Subject {name: 'Mechanical Engineering'})
      CREATE (ce:Subject {name: 'Chemical Engineering'})
      CREATE (se:Subject {name: 'Systems Engineering'})
      CREATE (ve:Subject {name: 'Civil Engineering'})
      CREATE (ee:Subject {name: 'Electrical Engineering'})

      CREATE (sd)-[:CURRICULUM]->(ph)
      CREATE (sd)-[:CURRICULUM]->(ch)
      CREATE (sd)-[:CURRICULUM]->(bi)
      CREATE (sd)-[:CURRICULUM]->(es)
      CREATE (md)-[:CURRICULUM]->(pm)
      CREATE (md)-[:CURRICULUM]->(am)
      CREATE (ed)-[:CURRICULUM]->(me)
      CREATE (ed)-[:CURRICULUM]->(se)
      CREATE (ed)-[:CURRICULUM]->(ce)
      CREATE (ed)-[:CURRICULUM]->(ee)
      CREATE (ed)-[:CURRICULUM]->(ve)

      CREATE (ph)-[:TAUGHT_BY]->(mrb)
      CREATE (ph)-[:TAUGHT_BY]->(mrk)
      CREATE (ch)-[:TAUGHT_BY]->(mrk)
      CREATE (ch)-[:TAUGHT_BY]->(mrsn)
      CREATE (bi)-[:TAUGHT_BY]->(mrsn)
      CREATE (bi)-[:TAUGHT_BY]->(mrsf)
      CREATE (es)-[:TAUGHT_BY]->(msn)
      CREATE (pm)-[:TAUGHT_BY]->(mrf)
      CREATE (pm)-[:TAUGHT_BY]->(mrm)
      CREATE (pm)-[:TAUGHT_BY]->(mrvdg)
      CREATE (am)-[:TAUGHT_BY]->(mrsg)
      CREATE (am)-[:TAUGHT_BY]->(mrspb)
      CREATE (am)-[:TAUGHT_BY]->(mrvdg)
      CREATE (me)-[:TAUGHT_BY]->(mrj)
      CREATE (ce)-[:TAUGHT_BY]->(mrsa)
      CREATE (se)-[:TAUGHT_BY]->(mrs)
      CREATE (ve)-[:TAUGHT_BY]->(msd)
      CREATE (ee)-[:TAUGHT_BY]->(mrsf)

      CREATE(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
      CREATE(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
      CREATE(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_106)
      CREATE(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
      CREATE(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_153)
      CREATE(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
      CREATE(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
      CREATE(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
      CREATE(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
      CREATE(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
      CREATE(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_043)
      CREATE(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
      CREATE(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
      CREATE(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_165)
      CREATE(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
      CREATE(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
      CREATE(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_062)
      CREATE(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_033)
      CREATE(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
      CREATE(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
      CREATE(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
      CREATE(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_121)
      CREATE(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_049)
      CREATE(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
      CREATE(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
      CREATE(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_085)
      CREATE(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
      CREATE(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
      CREATE(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
      CREATE(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
      CREATE(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
      CREATE(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
      CREATE(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
      CREATE(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
      CREATE(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
      CREATE(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
      CREATE(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_049)
      CREATE(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
      CREATE(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
      CREATE(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_149)
      CREATE(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
      CREATE(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
      CREATE(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
      CREATE(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
      CREATE(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
      CREATE(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
      CREATE(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_189)
      CREATE(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
      CREATE(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
      CREATE(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
      CREATE(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
      CREATE(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_148)
      CREATE(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
      CREATE(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
      CREATE(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
      CREATE(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
      CREATE(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_042)
      CREATE(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
      CREATE(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
      CREATE(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_034)
      CREATE(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_088)
      CREATE(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
      CREATE(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_88)
      CREATE(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
      CREATE(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
      CREATE(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
      CREATE(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
      CREATE(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
      CREATE(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
      CREATE(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
      CREATE(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
      CREATE(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
      CREATE(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_128)
      CREATE(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_149)
      CREATE(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
      CREATE(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_104)
      CREATE(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
      CREATE(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
      CREATE(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
      CREATE(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
      CREATE(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
      CREATE(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
      CREATE(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
      CREATE(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
      CREATE(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
      CREATE(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
      CREATE(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_128)
      CREATE(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
      CREATE(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
      CREATE(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_137)
      CREATE(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
      CREATE(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_156)
      CREATE(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
      CREATE(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
      CREATE(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
      CREATE(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_006)
      CREATE(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
      CREATE(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
      CREATE(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_001)
      CREATE(_101)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
      CREATE(_102)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
      CREATE(_103)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_136)
      CREATE(_104)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
      CREATE(_105)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_127)
      CREATE(_106)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
      CREATE(_107)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_036)
      CREATE(_108)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
      CREATE(_109)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
      CREATE(_110)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
      CREATE(_111)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
      CREATE(_112)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
      CREATE(_113)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_035)
      CREATE(_114)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
      CREATE(_115)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_070)
      CREATE(_116)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
      CREATE(_117)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
      CREATE(_118)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
      CREATE(_119)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
      CREATE(_120)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_167)
      CREATE(_121)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
      CREATE(_122)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
      CREATE(_123)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_004)
      CREATE(_124)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
      CREATE(_125)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
      CREATE(_126)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
      CREATE(_127)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_168)
      CREATE(_128)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
      CREATE(_129)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
      CREATE(_130)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
      CREATE(_131)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
      CREATE(_132)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
      CREATE(_133)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
      CREATE(_134)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_037)
      CREATE(_135)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_012)
      CREATE(_136)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
      CREATE(_137)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
      CREATE(_138)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
      CREATE(_139)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
      CREATE(_140)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_170)
      CREATE(_141)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
      CREATE(_142)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_139)
      CREATE(_143)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
      CREATE(_144)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
      CREATE(_145)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
      CREATE(_146)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
      CREATE(_147)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
      CREATE(_148)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
      CREATE(_149)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_155)
      CREATE(_150)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_151)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
      CREATE(_152)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
      CREATE(_153)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_179)
      CREATE(_154)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
      CREATE(_155)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_058)
      CREATE(_156)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_071)
      CREATE(_157)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
      CREATE(_158)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
      CREATE(_159)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
      CREATE(_160)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
      CREATE(_161)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
      CREATE(_162)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
      CREATE(_163)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_163)
      CREATE(_164)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
      CREATE(_165)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
      CREATE(_166)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_136)
      CREATE(_167)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
      CREATE(_168)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_110)
      CREATE(_169)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
      CREATE(_170)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
      CREATE(_171)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
      CREATE(_172)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
      CREATE(_173)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
      CREATE(_174)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
      CREATE(_175)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
      CREATE(_176)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
      CREATE(_177)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
      CREATE(_178)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_006)
      CREATE(_179)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
      CREATE(_180)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_185)
      CREATE(_181)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
      CREATE(_182)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
      CREATE(_183)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
      CREATE(_184)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
      CREATE(_185)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
      CREATE(_186)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
      CREATE(_187)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
      CREATE(_188)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
      CREATE(_189)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
      CREATE(_190)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_063)
      CREATE(_191)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_079)
      CREATE(_192)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_121)
      CREATE(_193)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
      CREATE(_194)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
      CREATE(_195)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
      CREATE(_196)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
      CREATE(_197)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
      CREATE(_198)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_039)
      CREATE(_199)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
      CREATE(_200)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
      CREATE(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
      CREATE(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_078)
      CREATE(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
      CREATE(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
      CREATE(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
      CREATE(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
      CREATE(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
      CREATE(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
      CREATE(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
      CREATE(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_147)
      CREATE(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
      CREATE(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_118)
      CREATE(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
      CREATE(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
      CREATE(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
      CREATE(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
      CREATE(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
      CREATE(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
      CREATE(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
      CREATE(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
      CREATE(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
      CREATE(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
      CREATE(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
      CREATE(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_087)
      CREATE(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_163)
      CREATE(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
      CREATE(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_042)
      CREATE(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_058)
      CREATE(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
      CREATE(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
      CREATE(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
      CREATE(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)
      CREATE(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
      CREATE(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_158)
      CREATE(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
      CREATE(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_102)
      CREATE(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
      CREATE(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
      CREATE(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
      CREATE(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
      CREATE(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
      CREATE(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_085)
      CREATE(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_025)
      CREATE(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
      CREATE(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_118)
      CREATE(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
      CREATE(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_071)
      CREATE(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
      CREATE(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
      CREATE(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
      CREATE(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
      CREATE(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_185)
      CREATE(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
      CREATE(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
      CREATE(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
      CREATE(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
      CREATE(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
      CREATE(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
      CREATE(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
      CREATE(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
      CREATE(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
      CREATE(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
      CREATE(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
      CREATE(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_153)
      CREATE(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
      CREATE(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
      CREATE(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
      CREATE(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
      CREATE(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
      CREATE(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
      CREATE(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
      CREATE(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
      CREATE(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
      CREATE(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_179)
      CREATE(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_005)
      CREATE(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
      CREATE(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_017)
      CREATE(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
      CREATE(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
      CREATE(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
      CREATE(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
      CREATE(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
      CREATE(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
      CREATE(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
      CREATE(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
      CREATE(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
      CREATE(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_064)
      CREATE(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
      CREATE(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_077)
      CREATE(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_124)
      CREATE(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
      CREATE(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
      CREATE(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
      CREATE(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_093)
      CREATE(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
      CREATE(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
      CREATE(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
      CREATE(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_043)
      CREATE(_101)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
      CREATE(_102)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
      CREATE(_103)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_040)
      CREATE(_104)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
      CREATE(_105)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_063)
      CREATE(_106)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_180)
      CREATE(_107)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
      CREATE(_108)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
      CREATE(_109)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
      CREATE(_110)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
      CREATE(_111)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
      CREATE(_112)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
      CREATE(_113)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
      CREATE(_114)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
      CREATE(_115)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_028)
      CREATE(_116)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
      CREATE(_117)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
      CREATE(_118)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
      CREATE(_119)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
      CREATE(_120)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
      CREATE(_121)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
      CREATE(_122)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_127)
      CREATE(_123)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
      CREATE(_124)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
      CREATE(_125)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
      CREATE(_126)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
      CREATE(_127)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
      CREATE(_128)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_165)
      CREATE(_129)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
      CREATE(_130)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_066)
      CREATE(_131)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
      CREATE(_132)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
      CREATE(_133)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
      CREATE(_134)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
      CREATE(_135)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
      CREATE(_136)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
      CREATE(_137)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
      CREATE(_138)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
      CREATE(_139)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
      CREATE(_140)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_013)
      CREATE(_141)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
      CREATE(_142)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
      CREATE(_143)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
      CREATE(_144)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_145)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
      CREATE(_146)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
      CREATE(_147)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_009)
      CREATE(_148)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
      CREATE(_149)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
      CREATE(_150)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
      CREATE(_151)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_055)
      CREATE(_152)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_157)
      CREATE(_153)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
      CREATE(_154)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
      CREATE(_155)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
      CREATE(_156)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
      CREATE(_157)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
      CREATE(_158)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_154)
      CREATE(_159)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
      CREATE(_160)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_168)
      CREATE(_161)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
      CREATE(_162)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
      CREATE(_163)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
      CREATE(_164)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_087)
      CREATE(_165)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_104)
      CREATE(_166)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
      CREATE(_167)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
      CREATE(_168)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
      CREATE(_169)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
      CREATE(_170)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_183)
      CREATE(_171)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_147)
      CREATE(_172)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
      CREATE(_173)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
      CREATE(_174)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_137)
      CREATE(_175)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
      CREATE(_176)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
      CREATE(_177)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_078)
      CREATE(_178)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
      CREATE(_179)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_062)
      CREATE(_180)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
      CREATE(_181)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
      CREATE(_182)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
      CREATE(_183)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
      CREATE(_184)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
      CREATE(_185)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
      CREATE(_186)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
      CREATE(_187)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
      CREATE(_188)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
      CREATE(_189)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_093)
      CREATE(_190)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_191)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_183)
      CREATE(_192)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
      CREATE(_193)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
      CREATE(_194)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
      CREATE(_195)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
      CREATE(_196)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
      CREATE(_197)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
      CREATE(_198)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_190)
      CREATE(_199)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
      CREATE(_200)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
      CREATE(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
      CREATE(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
      CREATE(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
      CREATE(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
      CREATE(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
      CREATE(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_066)
      CREATE(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
      CREATE(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
      CREATE(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
      CREATE(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
      CREATE(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
      CREATE(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_036)
      CREATE(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
      CREATE(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
      CREATE(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
      CREATE(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
      CREATE(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
      CREATE(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)
      CREATE(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_013)
      CREATE(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
      CREATE(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_026)
      CREATE(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_040)
      CREATE(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_064)
      CREATE(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
      CREATE(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_017)
      CREATE(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_159)
      CREATE(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
      CREATE(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
      CREATE(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_089)
      CREATE(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_157)
      CREATE(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
      CREATE(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
      CREATE(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
      CREATE(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
      CREATE(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_051)
      CREATE(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
      CREATE(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
      CREATE(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
      CREATE(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_023)
      CREATE(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
      CREATE(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
      CREATE(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
      CREATE(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
      CREATE(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
      CREATE(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
      CREATE(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
      CREATE(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_154)
      CREATE(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
      CREATE(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
      CREATE(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
      CREATE(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
      CREATE(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
      CREATE(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_160)
      CREATE(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_088)
      CREATE(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
      CREATE(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
      CREATE(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_110)
      CREATE(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_060)
      CREATE(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
      CREATE(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_030)
      CREATE(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_170)
      CREATE(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
      CREATE(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
      CREATE(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_004)
      CREATE(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
      CREATE(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_009)
      CREATE(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
      CREATE(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_077)
      CREATE(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
      CREATE(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
      CREATE(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
      CREATE(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
      CREATE(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_053)
      CREATE(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
      CREATE(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
      CREATE(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
      CREATE(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_134)
      CREATE(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
      CREATE(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
      CREATE(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
      CREATE(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_048)
      CREATE(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
      CREATE(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
      CREATE(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
      CREATE(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
      CREATE(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
      CREATE(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_070)
      CREATE(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_008)
      CREATE(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
      CREATE(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
      CREATE(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_180)
      CREATE(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_001)
      CREATE(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
      CREATE(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
      CREATE(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
      CREATE(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
      CREATE(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
      CREATE(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_100)
      CREATE(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 731  |
      | +relationships | 1247 |
      | +labels        | 6    |
      | +properties    | 230  |
