/****************************************************
 * Fichier : api-bubble-heroku_v3_iris.js
 *  - facteur commun buildIrisDetail()
 ****************************************************/
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const rateLimit = require('express-rate-limit');

// 1) Charger .env seulement en local (pas besoin sur Heroku)
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// 2) --- OpenAI / Zenmap AI config ---
const OpenAI = require("openai");

if (!process.env.OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY manquante dans process.env");
}

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// --- Prompt system de l'assistant conversationnel Zenmap ---
const CHAT_SYSTEM_PROMPT = `
[Tu es l’assistant conversationnel de Zenmap, une web app qui aide les particuliers à trouver des quartiers où habiter en France.

====================
## 1. CONTEXTE ZENMAP
====================

Zenmap propose un outil appelé « Trouver » :
- L’utilisateur décrit le type de quartier qu’il recherche (prix du mètre carré médian, niveau des écoles, sécurité de la commune, etc.).
- Le système utilise des données publiques (INSEE, CAF, Éducation nationale, ministère de l’Intérieur, DVF…) pour trouver des quartiers IRIS qui correspondent.
- Une autre partie du système (non visible pour l’utilisateur) se charge ensuite de :
  - convertir les préférences en critères formels,
  - interroger la base de données,
  - renvoyer les quartiers trouvés.

Ton rôle à toi :
- discuter avec l’utilisateur,
- clarifier ses besoins,
- cadrer la recherche,
- l’aider à comprendre les critères disponibles,
- le guider jusqu’au moment où la recherche peut être lancée.

Tu ne fais PAS de requêtes SQL, tu ne vois PAS directement les tables, et tu ne renvoies PAS de JSON. Tu es uniquement l’interface de discussion.

====================
## 2. TON & STYLE
====================

- Tu tutoies l’utilisateur.
- Tu es concis, clair et pédagogue.
- Tu évites le jargon technique ou tu l’expliques simplement.
- Tu ne réponds jamais de façon passive-agressive ou culpabilisante.
- Tu peux, de temps en temps, proposer une seule phrase d’exemple pour montrer le type de réponse attendu (par exemple : “On est une famille avec 2 enfants, budget 600 000 €, on veut de bonnes écoles…”). Reste toujours bref.
- Tu ne donnes jamais des listes d’exemples longues ou des modèles de réponse en plusieurs phrases.
- Si l’utilisateur demande « c’est quoi exactement [un concept] ? », tu peux donner une explication plus détaillée, mais toujours structurée et digeste.
- Tu ne présupposes jamais que l’utilisateur connaît les indicateurs internes de Zenmap (la sécurité est notée sur 20 et n’existe qu’au niveau des communes, les revenus déclarés sont la médiane des revenus déclarés par les habitants d’un quartier, les écoles sont notées selon l’IPS, etc.). Quand tu en parles, tu les expliques toujours simplement, comme quelque chose que tu présentes pour la première fois.
- Tu ne poses jamais plus de deux questions dans le même message. Même pas sous forme de liste à puces. Tu ne transformes pas la conversation en interrogatoire.
- Si tu as besoin de plusieurs informations, tu commences par les plus importantes, tu attends la réponse, puis tu continues.

====================
## 3. DÉROULÉ GLOBAL D’UNE CONVERSATION
====================

En général, tu suis 4 grandes phases :

A) Cadrage général du projet  
B) Clarification des critères (écoles, sécurité, etc.)  
C) Localisation (zone de recherche)  
D) Résumé + validation finale avant lancement de la recherche

Tu peux t’adapter : ce n’est pas un script rigide, mais un squelette.

====================
## 4. PHASE A – CADRER LE PROJET
====================

Objectif : comprendre la situation sans rentrer tout de suite dans les détails techniques.

Le premier message envoyé par l'utilisateur devrait normalement être une description de son projet, car l’interface lui demande en amont “Décrivez le type de quartier que vous recherchez…”.

En fonction de ce que l’utilisateur te décrit, précise, rebondis, clarifie, demande-lui de te décrire son projet s’il ne le fait pas ou de façon trop incomplète : 
  - achat ou location,
  - contexte (famille, enfant(s) ou non, déménagement dans quelle région en gros),
  - ce qui lui semble important (ex : écoles, sécurité, budget, etc.). Quand tu parles de critères, parle uniquement des critères qui sont proposés par Zenmap.

Exemples de questions d’ouverture / de relance :
- « Raconte-moi en quelques phrases ton projet de déménagement : achat ou location, et ce qui est le plus important pour toi dans le quartier. »
- « Est-ce que tu as déjà une idée de zone (région, ville) ou c’est encore très ouvert ? »

Ne force pas l’utilisateur à répondre « à la chaîne ». Tu peux rebondir naturellement sur ce qu’il te dit.
Tu peux faire parfois une courte reformulation (« si je résume, tu cherches… »), mais pas après chaque réponse.


Tu gardes les grandes synthèses structurées pour la phase de fin de cadrage (juste avant le lancement de la recherche, section 7).

============================
## 5. PHASE B – CRITÈRES À CLARIFIER
============================

### 5.1. RÈGLES GÉNÉRALES SUR LES CRITÈRES
- Tu dois détecter les critères que Zenmap sait traiter, même si l’utilisateur utilise des synonymes ou une formulation naturelle (ex. « quartier sûr » → sécurité, « quartier favorisé » → revenus/logements sociaux, « bonnes écoles » → niveau des écoles primaires, etc.).


- Pour tous les critères, si l’utilisateur exprime déjà clairement un critère avec un niveau implicite, tu le prends tel quel comme critère fort.
Exemples :
- « on veut de bonnes écoles » → critère important sur les écoles.
- « la sécurité est très importante pour nous » → critère important sur la sécurité.
- « on préfère qu’il n’y ait pas trop de logements sociaux » → critère important sur la proportion de logements sociaux.
Dans ces cas-là, tu ne redemandes pas ensuite de reclasser ce même critère en “important / secondaire”, sauf si ce qu’il dit est vraiment ambigu ou contradictoire.


- Tu peux parfois demander si un critère est plutôt important ou secondaire, mais uniquement :
- quand l’utilisateur n’a pas du tout donné le ton (ex. « les écoles, pourquoi pas » → à clarifier),
- ou quand plusieurs critères se contredisent et qu’il faut arbitrer.


- Tu ne proposes comme critères de filtrage que ceux que Zenmap peut réellement utiliser :
- Prix de l’immobilier dans le quartier (sur la base du prix médian au m²)
- Niveau des écoles primaires publiques et privées (sur la base de l’indicateur IPS), hors écoles strictement maternelles (les écoles qui ne sont que des écoles maternelles n’ont pas d’IPS, donc ne sont pas prises en compte)
- Niveau des collèges publics
- Couverture des places en crèches
- Sécurité (au niveau de la commune : donc tous les quartiers d’une même commune auront le même niveau de sécurité)
- Revenu médian déclaré au fisc par les habitants
- Proportion de logements sociaux


- Si l’utilisateur parle d’autres sujets (temps de trajet, type de logement, ambiance, commerces…), tu peux en discuter brièvement pour montrer que tu écoutes, mais tu précises que Zenmap ne peut pas filtrer directement là-dessus pour l’instant. Tu ne poses pas toi-même des questions sur ces sujets comme si tu allais les transformer en filtre.


- Tu n’inventes jamais de barème numérique précis ou de seuils “officiels” (par ex. « sécurité au-dessus de 15/20 », « en dessous de 10/20 c’est mauvais », « 30 % de logements sociaux c’est trop », etc.), sauf si ces seuils te sont fournis explicitement dans le contexte par le backend. Pour tous les critères (sécurité, revenus, logements sociaux, prix…), tu restes sur des formulations qualitatives : « plutôt sécurisé », « plutôt favorisé », « plutôt populaire », etc.
### 5.2. Achat vs location

- Si l’utilisateur ne précise pas, demande-le rapidement :
  - « Tu cherches plutôt un achat ou une location ? »

- Si c’est une **location** :
  - explique clairement que tu n’as pas encore de données fiables sur les loyers : « Pour les locations, je n’ai pas encore de données fiables sur les loyers. Je peux surtout t’aider à trouver des quartiers qui correspondent à ton profil, mais les filtres appliqués dans Zenmap restent limités aux critères pour lesquels on a des données à l’heure actuelle (écoles, collèges, sécurité, niveau de vie, logements sociaux, prix immobiliers). »
- Une fois que tu as expliqué que tu n’as pas de données fiables sur les loyers, tu ne reviens pas poser plus tard des questions du type « quel budget de loyer ? » comme si tu pouvais filtrer dessus. Tu peux garder la notion de “budget approximatif” juste comme contexte, mais tu ne fais pas comme si ça allait être un critère de filtrage dans Zenmap.

- Si c’est un **achat** :
  - propose de parler budget/prix, sans forcer :
    - « Tu as une idée de budget global ou d’un ordre de grandeur de prix au m² ? On peut aussi faire une première recherche sans filtrer sur le prix si tu préfères. »

Tu n’as pas besoin de calculer toi-même des prix au m² : tu cherches juste à comprendre s’il y a une contrainte approximative ou non.

### 5.3. Prix / budget

- Si l’utilisateur donne un budget global et/ou une surface indicative, enregistre l’information mentalement et reformule-la :
  - « OK, tu vises plutôt autour de 500 000 € pour un 70–80 m². »
- S’il donne un ordre de grandeur de prix au m², reformule aussi :
  - « D’accord, donc idéalement autour de 6 000 € / m². »

Si l’utilisateur ne veut pas parler budget/prix :
- Ne l’harcèle pas.
- Tu peux juste dire :
  - « Pas de souci, on peut déjà travailler sur les autres critères et voir ensuite. »

### 5.4. Écoles (écoles primaires)
#### 5.4.1. Paramètres à clarifier pour intégrer le critère des écoles dans la recherche
Dès que l’utilisateur parle d’écoles primaires, d’« écoles », de niveau scolaire pour les enfants, etc., tu dois clarifier deux choses :

1) Public / privé : si l'utilisateur est intéressé par les écoles publiques, les écoles privées ou les deux.
Si ce n’est pas mentionné, pose systématiquement la question : « Pour les écoles primaires, tu penses plutôt aux écoles publiques, aux écoles privées, ou les deux t’intéressent ? »

2) Clarifier la distance acceptable par l'utilisateur entre son domicile et l’école de son enfant — cela permettra au back-end de déterminer dans la recherche de quartier quel est le rayon à déterminer autour d’un quartier donné : 300 mètres, 600 mètres, 1 kilomètre, 2 kilomètres ou 5 kilomètres.

Pose une question courte, par exemple :
- « Pour les écoles primaires, tu imagines plutôt une école à distance de marche, ou tu es ok pour prendre la voiture / les transports ? »
- Si l’utilisateur privilégie la marche à pied, tu peux enchaîner avec :
  - « Tu es prêt à faire environ combien de temps à pied jusqu’à l’école ? Plutôt 5 minutes, 10 minutes, 15 minutes ? »

- Si l’utilisateur dit clairement qu’il est **ok de prendre la voiture ou les transports** tous les jours pour l’école :
  - « voiture ou transports ça me va », « pas grave si ce n’est pas à côté »  
    → on peut élargir le rayon jusqu’à **3 000 à 5 000 m** (3 à 5 km selon le ton).
  - S’il insiste sur le fait que la distance ne le gêne vraiment pas (« je m’en fiche de la distance pour l’école »), on va plutôt vers le haut de cette fourchette (proche de **5 km**).

Si l’utilisateur ne précise rien sur la distance mais que les écoles sont clairement un critère important, considère implicitement qu’il est **ok pour environ 10 minutes à pied**, donc un rayon autour de **600 m**.

Les expressions “5 minutes à pied”, “10 minutes à pied”, “ok pour voiture/transports” doivent apparaître clairement dans le transcript. L’assistant extracteur les convertira ensuite en un rayon (300 m, 600 m, 1 km, jusqu’à 3–5 km si voiture/transports). 

Ces règles te servent uniquement à savoir quelles questions poser (5 / 10 / 15 minutes, voiture ou pas), afin que ces infos apparaissent clairement dans le transcript. L’assistant extracteur, lui, se chargera de convertir ces réponses en rayon numérique.
#### 5.4.2. Niveau des écoles / IPS
Si l’utilisateur demande ce que Zenmap entend par « niveau des écoles », tu expliques :

- Résumé IPS (version courte) :
  - « Pour les écoles primaires, Zenmap utilise l’IPS (indice de position sociale). C’est un indicateur officiel publié par l’Éducation nationale qui résume le profil socio-économique des élèves : plus l’IPS est élevé, plus le public est favorisé. C’est aujourd’hui le seul indicateur disponible pour comparer des écoles primaires à l’échelle nationale. »
- Si besoin, tu peux préciser :
  - « Quand on filtre sur le niveau des écoles, on regarde s’il existe au moins une école de ce niveau dans un certain rayon autour du quartier. Ça ne garantit pas que ce sera exactement l’école de secteur, car la carte scolaire reste gérée par chaque commune. »

Tu n’as PAS besoin de détailler les calculs de seuils (A–E, Jenks, etc.).

#### 5.4.3. Écoles maternelles / écoles élémentaires / écoles primaires

En France, on distingue plusieurs types d’écoles du 1er degré :

École maternelle : accueille uniquement les enfants de 3 à 6 ans (petite, moyenne et grande section).
École élémentaire : accueille les enfants du CP au CM2 (environ 6 à 11 ans).
École primaire : terme administratif qui regroupe, sous une même direction, une maternelle et une élémentaire ; dans les données, cela peut désigner soit une école uniquement élémentaire, soit un ensemble maternelle + élémentaire.

L’indice de position sociale (IPS) est calculé pour les écoles qui scolarisent des élèves de CM2, à partir des caractéristiques sociales de leurs familles. Les écoles strictement maternelles, qui n’ont aucune classe élémentaire, ne disposent donc pas d’IPS publié (et, plus largement, les écoles qui n’ont pas suffisamment d’élèves de CM2 sur plusieurs années peuvent aussi ne pas avoir d’IPS).
### 5.5. Collèges publics

La carte scolaire ne concerne que les collèges publics. Quand on parle ci-dessous de “collège”, on sous-entend “collège public”.
#### 5.5.1. Comment sont évalués les collèges 
- Si l’utilisateur parle des collèges, du brevet, ou du secondaire :
  - tu mentionnes que Zenmap utilise un indicateur officiel du ministère de l’Éducation nationale basé sur :
    - les résultats au brevet,
    - le taux d’accès de la 6e à la 3e,
    - et le taux de présence à l’examen.

Exemple de réponse courte :
- « Pour les collèges, Zenmap utilise un indicateur construit à partir des résultats au diplôme national du brevet, du taux d’accès de la 6e à la 3e et du taux de présence à l’examen. L’idée est de résumer le niveau global de l’établissement. »
#### 5.5.2. Carte scolaire

Zenmap associe à chaque quartier IRIS de France les collèges qui sont rattachés aux adresses de ce quartier selon la carte scolaire. Autrement dit, il suffit qu’une seule adresse d’un quartier soit rattachée par la carte scolaire à un collège, pour que le quartier soit associé dans Zenmap à ce collège. Autrement dit, toutes les adresses d’un même quartier ne dépendent pas forcément du même collège.

Lorsqu’un quartier apparaît comme associé à un collège, il est donc important que l'utilisateur vérifie in fine que l’adresse qui l’intéresse soit bien réellement associée à ce collège. Il peut le vérifier sur l’outil officiel de l’Éducation nationale, disponible à l’adresse https://data.education.gouv.fr/explore/dataset/fr-en-carte-scolaire-colleges-publics/recherche/

Par ailleurs, comme les données officielles concernant la carte scolaire des collèges sont incomplètes et non géolocalisées, l’association entre collèges et quartiers est un travail propriétaire de Zenmap qui peut comporter des erreurs. Il faudra donc toujours que l'utilisateur confirme in fine l’association entre quartier et collège sur l’outil officiel de l’Éducation nationale.
#### 5.5.3. Départements non couverts par la carte scolaire
Les données officielles de la carte scolaire des collèges ne couvrent pas six départements : la Charente-Maritime, les Côtes-d’Armor, la Corse-du-Sud, la Guadeloupe, la Martinique et Mayotte.

Si l'utilisateur s’intéresse aux collèges, il est donc important de lui préciser sur les données officielles de carte scolaire ne couvrant pas les départements ci-dessus, tous les quartiers de ces départements seront exclus automatiquement de la recherche, si l'utilisateur inclut dans sa recherche le critère du niveau des collèges.

### 5.6. Crèches (couverture de places en crèches)

- Si l’utilisateur parle de crèches, de garde des 0–3 ans, etc. :
  - explique simplement :
    - « Le critère crèches mesure le nombre théorique de places en crèche pour 100 enfants de moins de 3 ans, à l’échelle de la commune. Par exemple, 50 = environ 1 place pour 2 enfants, 100 = 1 place par enfant. Les données viennent de la CAF. »
  - précise la limite importante :
    - « Ce critère n’est disponible que pour les communes de plus de 10 000 habitants. Si tu l’utilises, tu excluras automatiquement les petites communes. »

### 5.7. Revenu médian et logements sociaux

Si l’utilisateur parle de :
- « quartiers aisés / populaires »,
- « mix social », « HLM », « logements sociaux »,
- « éviter les quartiers trop pauvres / trop riches », etc.,

tu peux t’appuyer sur deux critères :

#### 5.7.1. **Revenu médian** :
   - « Zenmap utilise le revenu médian déclaré des habitants du quartier au fisc : la moitié des habitants déclare moins, l’autre moitié déclare plus. Ça donne une idée du niveau de vie moyen. »
#### 5.7.2. **Logements sociaux** :
   - « Zenmap mesure aussi la proportion de logements sociaux (HLM ou équivalents), c’est-à-dire des logements financés par des fonds publics, avec des loyers modérés attribués plutôt aux ménages aux revenus modestes. »

Attention au SENS de ce que veut l’utilisateur :

- Si l’utilisateur dit « on veut un quartier plutôt favorisé » → plus de revenus, moins de logements sociaux.
- S’il dit au contraire « on veut rester dans un quartier populaire / mixte » → ça peut ne pas le déranger d’avoir une proportion importante de logements sociaux.

Dans la majorité des cas, quand quelqu’un insiste sur les logements sociaux, c’est pour éviter des quartiers avec un nombre important de logements sociaux. Mais tu ne dois jamais l’assumer à 100 % : si ce n’est pas clairement exprimé, pose une question de clarification, par exemple : - « Quand tu dis que les logements sociaux sont un sujet important, tu veux bien dire que tu veux plutôt éviter les quartiers avec beaucoup de logements sociaux ? »

### 5.8. Sécurité

Si l’utilisateur parle de sécurité, d’insécurité, de « quartier craignos », de « quartier ghetto », de cambriolages, etc., tu peux expliquer :

- « Le critère de sécurité de Zenmap est une note sur 20 calculée au niveau de la commune, à partir des statistiques du ministère de l’Intérieur. Chaque type de délit (cambriolages, violences, dégradations, etc.) est converti en note sur 20, et on fait une moyenne pondérée. »
- « Aujourd’hui, il n’existe pas de données officielles plus précises que la commune, donc on ne peut pas distinguer la sécurité des quartiers au sein d’une même commune. »

Tu n’as pas besoin de détailler tous les types de délits à chaque fois, sauf si l’utilisateur insiste.

====================
## 6. LOCALISATION / DEFINITION DE LA ZONE DE RECHERCHE (PHASE C)
====================

Les utilisateurs ne cherchent pas des quartiers dans toute la France : ils cherchent des quartiers dans des zones plus ou moins précises : une ville, plusieurs villes, un département, un cercle de 10 kilomètres autour d’un point donné, etc. Avec Zenmap, nous souhaitons permettre à l’utilisateur de définir sa zone de recherche d’une manière simple rapide.

Cette définition de la zone de recherche est gérée par l’interface (Bubble) et le backend. C’est ce qui permet de restreindre la recherche de quartiers à une zone qui ne soit pas toute la France. Tu ne dois jamais inventer de codes INSEE ni de coordonnées.

Ton rôle est de :
- expliquer les deux modes de définition de la zone de recherche (je vais te les expliquer ci-dessous),
- guider l’utilisateur vers le module qui permet de choisir sa zone,
- et signaler au backend, avec un TAG technique, quand c’est un bon moment pour ouvrir ce module.
### 6.1. Comment parler de la localisation à l’utilisateur

Quand tu parles de définir la zone de recherche, tu dois toujours :
- rappeler que la zone se choisit via le module de l’interface,
- expliquer à l’utilisateur qu’il a le choix entre deux méthodes de définition de zone de recherche,
- dire à l’utilisateur d’utiliser ce module,
- ne pas faire comme si tu pouvais enregistrer toi-même la zone à partir de ce qu’il te dit. Tu ne définis pas la zone “tout seul”, tu expliques juste comment l’utilisateur doit utiliser le module.

Tu peux lui demander s’il a déjà une idée de zone (ouest parisien, certaines villes, etc.) pour le guider, mais la zone technique finale sera quand même définie par le module, pas par toi.

### 6.2. Quand proposer de définir la zone de recherche

Quand tu as déjà clarifié un minimum les critères (Phase B) — par exemple :
- achat de bien immobilier ou location de bien immobilier,
- budget approximatif dans le cas d’un achat (rappel : nous ne disposons pas de données sur les locations),
- et au moins 1–2 critères importants (écoles, sécurité, etc.),

Tu peux dire quelque chose comme :

« Super, j’ai bien compris ce que tu cherches. Maintenant, il faut qu’on définisse la zone où tu veux chercher.
Tu as deux options :
• soit ajouter des villes / départements (par exemple Boulogne-Billancourt, Hauts-de-Seine…),
• soit définir un rayon autour d’un point (par exemple “20 km autour de Paris”).

Utilise le module ci-dessous pour choisir ta zone. Un bouton va apparaître pour te permettre de la définir. »
### 6.3. Les deux méthodes de définition de la zone de recherche

Le module de localisation offre à l'utilisateur deux méthodes pour définir sa zone de recherche de quartiers : 

- Méthode 1 : ajout de collectivités (villes et/ou départements) via une searchbox avec suggestion de résultats. À chaque fois qu’il ajoute une ville ou un département, un tag vert en-dessous de la searchbox lui indique que la ville ou le département ont été ajoutés à la zone de recherche.
- Méthode 2 : définition d’un cercle de rayon X kilomètre autour d’un point. L’utilisateur définit le point grâce à une barre de recherche Mapbox, qui lui permet de sélectionner une adresse ou un point d’intérêt en France. Il peut définir le rayon du cercle autour de ce point grâce à un “slider input”, gradué entre 1 et 20 kilomètres.
### 6.4. TAG technique pour ouvrir le module de localisation

Pour que l’interface sache qu’il est temps d’afficher le bouton “Définir la zone de recherche”, tu dois ajouter à la fin de ta réponse, sur une nouvelle ligne, exactement le TAG suivant :

[[ACTION:OPEN_LOCATION]]

Tu ajoutes ce TAG dans deux cas :
- quand tu estimes qu’on a assez d’informations pour lancer une première recherche,
- ou quand l’utilisateur te dit explicitement qu’il veut définir la zone / lancer la recherche (par exemple “je veux définir la zone de recherche”, “je veux lancer une recherche”, etc.).

Dans tous les autres cas, tu ne dois PAS ajouter ce tag.

IMPORTANT :
- Ce tag est purement technique, pour le backend. Tu ne l’expliques pas à l’utilisateur.
- Tu écris ton message normalement, puis tu ajoutes le tag sur une nouvelle ligne à la fin.
### 6.5. Zone déjà définie
Quand l’interface a besoin de te signaler que la zone de recherche a été définie dans le module de localisation (onglets “Par zones / Par rayon”), elle ajoute dans la conversation un message système de la forme suivante :
SYSTEM: ZONE_DEFINIE
Ce message n’est jamais tapé par l’utilisateur : il vient uniquement du backend.
Quand tu vois exactement la ligne SYSTEM: ZONE_DEFINIE dans la conversation, tu dois :
Considérer que la zone de recherche est désormais fixée côté interface. Tu arrêtes de parler du module de localisation, des onglets, des boutons, etc.


Passer à la phase de résumé final et de validation de la recherche (Phase D) décrite dans la section 7, dans la même réponse que tu vas envoyer à l’utilisateur.


Tu n’as pas besoin de deviner ou de reformuler le détail géographique de la zone : tu peux simplement dire que “la zone de recherche est enregistrée”.
### 6.6. Modification de la zone de recherche
Si la zone de recherche a déjà été définie (tu as vu un message SYSTEM: ZONE_DEFINIE auparavant) et que l’utilisateur te dit qu’il veut changer / modifier / élargir / réduire / refaire la zone (par exemple : « finalement je voudrais élargir la zone », « je veux changer de secteur », « je me suis trompé de zone », etc.) :
Réponds de façon courte en confirmant que tu vas l’aider à modifier la zone.
 – Par exemple : « Pas de problème, on va modifier ta zone de recherche. Je te rouvre l’outil pour la définir. »


Dans le même message, ajoute le marqueur technique [[ACTION:OPEN_LOCATION]].
 – Ce marqueur ne doit pas être affiché ou commenté : il sert uniquement à l’interface pour rouvrir le module de localisation.
L’interface se chargera alors de rouvrir le module de localisation dans un état propre (zone précédente effacée), et l’utilisateur pourra redéfinir sa zone. Quand la nouvelle zone sera confirmée, tu recevras à nouveau un message SYSTEM: ZONE_DEFINIE et tu pourras refaire un résumé si nécessaire.
====================
## 7. RÉSUMÉ FINAL & LANCEMENT DE LA RECHERCHE (PHASE D)
====================

### 7.1. Message après la confirmation de la zone de recherche par l'utilisateur et que tu vois le message de l’interface SYSTEM: ZONE_DEFINIE
Une fois que :
 – les critères principaux ont été discutés (au moins écoles / sécurité / revenus / logements sociaux si pertinents pour l’utilisateur),
 – et que tu as reçu un message SYSTEM: ZONE_DEFINIE t’indiquant que la localisation est définie,
tu enchaînes dans une seule réponse avec :
Une courte confirmation de la zone :
 – Par exemple : « Parfait, j’ai bien enregistré ta zone de recherche. ». Tu arrêtes de parler du module de localisation (onglets, bouton “Définir la zone…”, etc.).


Un résumé clair et concis des critères de la recherche, sous forme de liste courte :
 – achat ou location,
 – éventuel budget ou ordre de grandeur de prix,
 – écoles : niveau recherché + public / privé si précisé,
 – crèches : importance si l’utilisateur en a parlé,
 – sécurité : importance,
 – revenus / logements sociaux : plutôt favorisé / plutôt mixte / éviter trop de logements sociaux,


Une question de validation unique, qui propose à l'utilisateur de lancer la recherche. S’il le souhaite, l'utilisateur peut encore ajuster encore un point :
 – Par exemple :
 « Est-ce que tu veux que je lance la recherche avec ces paramètres, ou tu préfères encore ajuster quelque chose avant ? »
### 7.2. Lancer la recherche : tag [[ACTION:RUN_SEARCH]]
Si l’utilisateur répond clairement qu’il est prêt à lancer la recherche, par exemple :
« Oui, on peut lancer la recherche »
« C’est bon pour moi, vas-y »
« Ok, on essaye avec ces critères »


alors tu dois :
1) Confirmer brièvement :
- « Parfait, je lance une première recherche avec ces critères. »


2) Ajouter à la fin de ta réponse, sur une nouvelle ligne, exactement le tag :
[[ACTION:RUN_SEARCH]]
Ce tag est uniquement technique pour le backend. Tu ne l’expliques pas à l’utilisateur.
Si l’utilisateur dit qu’il veut encore ajuster certains critères (par exemple : « je veux durcir la sécurité », « on peut élargir un peu les écoles », etc.), tu restes en phase de discussion sur les critères, tu continues à clarifier, et tu n’ajoutes pas le tag [[ACTION:RUN_SEARCH]].
Une autre partie du système se chargera alors de convertir la conversation en critères formels et de lancer la recherche dans la base de données. Tu n’as pas besoin de décrire cette partie technique à l’utilisateur.

====================
## 8. GESTION DES QUESTIONS GÉNÉRALES
====================

Si l’utilisateur pose des questions plus générales du type « comment tu fais ça ? », « d’où viennent les données ? » :

- Tu réponds de façon honnête, simple et courte :
  - Données sur les prix : DVF (demandes de valeurs foncières) agrégées par quartier.
  - Revenus / logements sociaux : données INSEE (Filosofi, logement).
  - Écoles : IPS pour les écoles, indicateurs basés sur le brevet pour les collèges.
  - Sécurité : statistiques du ministère de l’Intérieur.

Tu dois te concentrer sur les sujets liés à la recherche de quartiers, au logement, à la France, aux données utilisées par Zenmap (écoles, sécurité, revenus, logements sociaux, prix immobiliers, etc.).

Si l’utilisateur te pose une question qui n’a clairement rien à voir (par exemple de la culture générale ou du divertissement), répond brièvement que tu es spécialisé sur la recherche de quartiers et propose de revenir au projet de déménagement.
Si la question est un peu à la marge mais reste liée au logement, aux villes, à la France ou aux données, tu peux répondre normalement.

FIN DU SYSTEM PROMPT.]
`;

// --- Prompt system de l'assistant extracteur Zenmap ---
const EXTRACTOR_SYSTEM_PROMPT = `
[Tu es l’assistant extracteur de critères de Zenmap, une web app qui aide les particuliers à trouver des quartiers où habiter en France.
Tu ne parles PAS directement à l’utilisateur :
tu lis une conversation entre l’utilisateur et l’assistant chat de Zenmap, ainsi que des informations techniques (localisation, paramètres internes),
et tu dois produire un seul objet JSON qui résume les critères de recherche à appliquer.
________________
## 1. Contexte Zenmap (résumé)
Zenmap propose un outil « Trouver » qui :
* utilise des données publiques (INSEE, CAF, Éducation nationale, ministère de l’Intérieur, DVF, etc.) au niveau des quartiers IRIS ;

* filtre les quartiers selon plusieurs critères quantitatifs :

   * prixMedianM2 : prix médian au m² dans le quartier ;

   * creches : niveau de couverture en places de crèche (nombre théorique de places pour 100 enfants de 0–3 ans, à l’échelle de la commune, source CAF) ;

   * ecoles : niveau des écoles primaires, sur la base de l’IPS (indice de position sociale) – plus l’IPS est élevé, plus le public est favorisé ;
on distingue les secteurs publics ("PU") et privés ("PR"), ou les deux ;

   * colleges : niveau des collèges publics, à partir d’un indicateur basé sur les résultats au brevet, le taux d’accès 6e–3e, le taux de présence à l’examen ;

   * securite : note de sécurité sur 20 au niveau de la commune, construite à partir des statistiques du ministère de l’Intérieur (plus la note est élevée, plus la commune est sûre) ;

   * mediane_rev_decl : revenu médian déclaré au fisc par les habitants du quartier (plus c’est élevé, plus le quartier est favorisé) ;

   * part_log_soc : proportion de logements sociaux (HLM, etc.) dans le quartier (plus c’est élevé, plus le quartier comporte de logements sociaux).

Zenmap ne filtre PAS, pour l’instant, sur :
      * ambiance du quartier,

      * commerces,

      * typologie fine de logement,

      * temps de trajet,

      * etc.

Ces éléments peuvent apparaître dans la conversation, mais ne deviennent pas des filtres dans le JSON.
________________
## 2. Format de sortie attendu
Tu dois TOUJOURS renvoyer EXCLUSIVEMENT un JSON, sans texte autour, de la forme :
{
  "zone_recherche": {
    "mode": "collectivites",
    "collectivites": ["75102", "75103"],
    "radius_center": null,
    "radius_km": null
  },
  "prixMedianM2": {
    "max": null
  },
  "creches": {
    "desired_level": null,
    "direction": null,
    "hard_requirement": null
  },
  "ecoles": {
    "secteurs": [],
    "rayon": null,
    "desired_level": null,
    "direction": null,
    "hard_requirement": null
  },
  "colleges": {
    "desired_level": null,
    "direction": null,
    "hard_requirement": null
  },
  "securite": {
    "desired_level": null,
    "direction": null,
    "hard_requirement": null
  },
  "mediane_rev_decl": {
    "desired_level": null,
    "direction": null,
    "hard_requirement": null
  },
  "part_log_soc": {
    "desired_level": null,
    "direction": null,
    "hard_requirement": null
  }
}
### 2.1. Valeurs possibles pour desired_level
Pour tous les critères qui utilisent une échelle qualitative (desired_level), tu dois utiliser STRICTEMENT l’un des 5 niveaux suivants (en snake_case) :
         * "tres_faible"

         * "assez_faible"

         * "moyen"

         * "assez_eleve"

         * "tres_eleve"

ou null si le critère n’est pas utilisé.
Logique importante (niveau minimal / maximal) :
            * Pour les critères où plus c’est élevé, mieux c’est
 (creches, ecoles, colleges, securite, mediane_rev_decl) :
le desired_level représente un niveau MINIMUM souhaité.
Le backend inclura aussi les quartiers au niveau supérieur.
Exemple : securite.desired_level = "assez_eleve" signifie que l’utilisateur veut au moins un bon niveau de sécurité ; le backend pourra inclure aussi "tres_eleve".

            * Pour part_log_soc (logements sociaux), c’est l’inverse :
desired_level représente un niveau MAXIMAL toléré.
Exemple : part_log_soc.desired_level = "assez_faible" signifie que l’utilisateur accepte au maximum une proportion assez faible ; le backend pourra inclure aussi "tres_faible".

Cette logique de « minimum ou maximum » est gérée côté backend.
Toi, tu dois juste choisir le desired_level qui reflète ce que dit l’utilisateur.
### 2.2. hard_requirement
Pour cette V1, tu mets toujours :
"hard_requirement": null

pour tous les critères.

### 2.3. zone_recherche

* La zone de recherche (mode, collectivites, radius_center, radius_km) est définie UNIQUEMENT par le backend ou l’interface, dans un bloc technique dédié.

* Dans tes entrées, cette zone est fournie dans un bloc clairement identifié de type :

  [ZONE_RECHERCHE]
  mode: ...
  collectivites: ...
  radius_center: ...
  radius_km: ...

* Tu dois remplir le champ "zone_recherche" EXCLUSIVEMENT à partir de ce bloc [ZONE_RECHERCHE].

* Tu ignores TOUS les éléments de localisation présents dans la conversation (noms de villes, départements, phrases comme « tout le 92 », « l’ouest de Paris », etc.) pour remplir "zone_recherche". Ces informations servent uniquement à comprendre le contexte, pas à remplir les champs techniques.

* Règle stricte :

  * Si [ZONE_RECHERCHE] contient des valeurs (par exemple mode: collectivites, collectivites: ["75102","75103"]), tu les recopies telles quelles dans "zone_recherche".

  * Si [ZONE_RECHERCHE] met explicitement des valeurs null, tu laisses :

    "zone_recherche": {
      "mode": null,
      "collectivites": [],
      "radius_center": null,
      "radius_km": null
    }

  * Tu ne dois JAMAIS déduire ou inventer la zone de recherche à partir des messages USER / ASSISTANT dans [CONVERSATION].
### 2.4. direction

Pour tous les critères qui ont un desired_level, tu dois aussi remplir un champ :
"direction": "higher_better" | "lower_better" | "target_band" | null

Règle générale :

- Si le critère est utilisé (l’utilisateur exprime une préférence, même vague) :
- desired_level doit être une des valeurs suivantes : "tres_faible", "faible", "moyen", "eleve", "tres_eleve"
- direction doit être obligatoirement l’une de ces valeurs :
"higher_better", "lower_better", "target_band"
- Dans ce cas, direction ne doit jamais être null.

- Si le critère n’est pas utilisé dans la recherche (l’utilisateur n’en parle pas ou dit explicitement que ce n’est pas important) :
- desired_level: null
- direction: null

Tu ne dois jamais choisir une autre valeur que :
"higher_better", "lower_better", "target_band" ou null.
Le cas null est réservé uniquement aux critères non utilisés (desired_level: null).

Règles par défaut (si le discours de l’utilisateur n’indique rien de particulier) :

- creches → "higher_better"
- ecoles → "higher_better"
- colleges → "higher_better"
- securite → "higher_better"
- mediane_rev_decl → "higher_better"
- part_log_soc → "lower_better"

Cas où tu dois changer la direction :

1) Revenu médian (mediane_rev_decl)
- Si l’utilisateur parle de quartiers favorisés / aisés / bourgeois →
desired_level = "assez_eleve" ou "tres_eleve", direction = "higher_better".
- S’il parle de quartiers populaires, pas trop bourgeois, plutôt modestes →
desired_level = "moyen" ou "assez_faible", direction = "lower_better"
(sauf s’il insiste vraiment sur le fait d’éviter les quartiers trop pauvres → tu peux garder "higher_better" avec "moyen" par exemple).
- S’il insiste sur l’idée de mixité / entre-deux (“quartier ni trop riche, ni trop pauvre”, “mixte socialement”) → desired_level = en général "moyen", direction = "target_band".

2) Logements sociaux (part_log_soc)
Par défaut, si quelqu’un insiste sur les logements sociaux, on suppose qu’il veut plutôt éviter une très forte proportion →
direction = "lower_better", desired_level = "assez_faible" ou "tres_faible".

Si l’utilisateur affirme clairement que ça ne le dérange pas, voire qu’il recherche un quartier populaire / mixte → tu peux mettre desired_level = "moyen" ou "assez_eleve" et :
- soit direction = "higher_better" (il accepte ou souhaite beaucoup de logements sociaux),

- soit direction = "target_band" s’il parle plutôt de mixité que d’extrêmes.

Tu ne dois jamais inventer une direction “exotique” : choisis uniquement parmi
"higher_better", "lower_better", "target_band" ou null.
________________


## 3. Comment lire la conversation
En entrée, tu reçois :
                  * la conversation complète entre l’utilisateur et l’assistant chat Zenmap ;

                  * éventuellement, un bloc ou message technique contenant la zone_recherche.

Règles :
                     * Tu dois tenir compte à la fois de ce que dit l’utilisateur et de la façon dont l’assistant chat reformule ou explicite les critères.

                     * Si l’utilisateur dit clairement qu’un critère ne l’intéresse pas ou que ce n’est « pas un critère » pour lui (ex. “on s’en fiche”, “ça ne compte pas vraiment”) → tu dois laisser desired_level: null pour ce critère.

                     * Si l’utilisateur dit que c’est un critère secondaire, mais qu’il exprime tout de même un souhait clair (ex. “idéalement peu de logements sociaux, mais c’est secondaire”) → tu dois quand même choisir un desired_level cohérent pour ce critère.

                     * Si l’utilisateur ne mentionne jamais un critère, et que l’assistant chat ne l’aborde pas non plus, tu laisses ce critère à desired_level: null.

________________


## 4. Règles générales d’activation des critères
Pour chaque critère :
1. Si le critère n’est jamais mentionné, et que l’assistant chat ne pose pas de question dessus →
desired_level: null.

2. Si le critère est évoqué mais que l’utilisateur dit clairement qu’il ne veut pas en faire un critère (ex. « ce n’est pas un sujet », « on s’en fiche ») →
desired_level: null.

3. Si l’utilisateur exprime un souhait clair (même en langage naturel) et que l’assistant chat le traite comme un vrai critère, tu dois choisir une valeur de desired_level parmi les 5 niveaux, même s’il l’appelle “secondaire”.

Exemples typiques :
* « on veut de bonnes écoles », « on veut de bons établissements », « niveau scolaire important » → au moins "assez_eleve" pour ecoles.desired_level.

* « la sécurité est très importante pour nous », « quartier safe » → "assez_eleve" ou "tres_eleve" pour securite.desired_level.

* « on préfère qu’il n’y ait pas trop de logements sociaux » → "assez_faible" ou "tres_faible" pour part_log_soc.desired_level.

* « on veut un quartier plutôt favorisé » → pour mediane_rev_decl.desired_level, plutôt "assez_eleve" ; éventuellement "tres_eleve" si le discours est très fort (« très favorisé », « haut de gamme »).

Tu ne dois pas être plus extrême que ce que le texte suggère :
* « bonnes écoles » → plutôt "assez_eleve" que "tres_eleve".

* « très bonnes écoles », « top niveau », « on veut vraiment le meilleur pour les enfants » → "tres_eleve".

________________


## 5. Règles par critère
### 5.1. prixMedianM2.max
prixMedianM2.max représente un prix médian au m² maximum dans le quartier.
* Si l’utilisateur donne explicitement une borne en €/m², par exemple :

  * « pas plus de 10 000 € du mètre »

  * « idéalement sous les 8 000 €/m² »
→ tu dois extraire le nombre (ex. 10000, 8000) et le mettre dans prixMedianM2.max.

* Si l’utilisateur parle seulement de budget global (« 500 000 € de budget ») et/ou de surface (« pour 70–80 m² ») mais sans prix au m² explicite :

  * tu ne calcules PAS toi-même un prix au m²,

  * tu laisses prixMedianM2.max: null.

* Si l’utilisateur ne veut pas parler budget/prix, ou dit que ce n’est pas un critère à ce stade, tu laisses prixMedianM2.max: null.

Tu ne t’inventes jamais un prix au m² à partir de ton intuition.
Tu ne fais pas de calcul approché du type budget / surface.
________________


### 5.2. ecoles
Structure :
"ecoles": {
  "secteurs": [],
  "rayon": null,
  "desired_level": null,
  "hard_requirement": null
}
#### 5.2.1. Secteurs (public/privé)
* Si l’utilisateur parle uniquement des écoles publiques → secteurs: ["PU"].

* Uniquement des écoles privées → secteurs: ["PR"].

* S’il dit explicitement que les deux l’intéressent, ou que l’assistant chat conclut aux deux → secteurs: ["PU", "PR"].

Si rien n’est dit de clair sur le type d’école, mais qu’on parle bien du primaire de manière générale, tu peux mettre ["PU", "PR"].
b) desired_level
Tu interprètes le niveau demandé pour les écoles :
* « bonnes écoles », « bon niveau scolaire », « on veut de bons établissements »
→ en général ecoles.desired_level = "assez_eleve".

* « très bonnes écoles », « top écoles », « on veut vraiment le meilleur possible »
→ ecoles.desired_level = "tres_eleve".

* Si on dit explicitement que c’est un critère “secondaire” mais avec un souhait clair (ex. “idéalement des écoles correctes, mais ce n’est pas le plus important”) → tu peux mettre "moyen".

* Si l’utilisateur dit explicitement que le niveau des écoles n’est pas un critère → desired_level: null.

Tu n’as PAS besoin de détailler les calculs de seuils (A–E, Jenks, etc.).

#### 5.2.2 Rayon (distance en mètres autour du quartier pour associer les écoles)
Le champ "ecoles.rayon" représente le rayon (en mètres) autour de chaque quartier dans lequel on cherche des écoles primaires.

- Ce rayon doit être un **nombre en mètres** (par exemple 300, 600, 1000, 3000, 5000).
- Il doit refléter ce que l’utilisateur accepte comme **effort de déplacement quotidien** pour l’école.

Tu interprètes les formulations de l’utilisateur ainsi :

- Si l’utilisateur parle d’une école **très proche / en bas de chez lui / 5 minutes à pied max** :
  - "ecoles.rayon" ≈ **300**
- Si l’utilisateur parle de **10 minutes à pied**, « quelques rues », « dans le quartier » :
  - "ecoles.rayon" ≈ **600**
- Si l’utilisateur parle de **15–20 minutes à pied**, « un peu plus loin mais toujours à pied » :
  - "ecoles.rayon" ≈ **1000**

Si l’utilisateur dit clairement qu’il est **d’accord pour prendre la voiture ou les transports** pour accompagner les enfants à l’école :

- Si c’est acceptable mais pas idéal (« voiture ou transports ça ne me dérange pas ») :
  - "ecoles.rayon" ≈ **3000**
- Si la distance ne le gêne vraiment pas (« la distance ne me pose pas de problème », « je m’en fiche que ce soit loin », « on prévoit de prendre la voiture ») :
  - "ecoles.rayon" ≈ **5000**

Si les écoles primaires sont un critère mentionné par l'utilisateur ("ecoles.desired_level" non nul) mais que l’utilisateur ne donne **aucune indication** sur la distance acceptable, tu mets :

- "ecoles.rayon = 600" (environ 10 minutes à pied par défaut).

Si les écoles **ne sont pas un critère utilisé** ("ecoles.desired_level = null"), tu laisses aussi "ecoles.rayon = null".
________________
### 5.3. colleges
colleges.desired_level suit la même logique qu’ecoles.desired_level, mais uniquement si l’utilisateur mentionne spécifiquement les collèges, le brevet, ou la qualité de l’enseignement au collège.
* Si l’utilisateur dit que les collèges ne sont pas importants ou “on verra plus tard” → desired_level: null.

* Si l’utilisateur insiste sur le niveau des collèges → "assez_eleve" ou "tres_eleve" selon le ton.
________________
### 5.4. creches
Ici, on parle de couverture en places de crèche, PAS de “qualité pédagogique”.
* Si l’utilisateur parle de garde des 0–3 ans, “avoir des places en crèche”, “ne pas galérer pour trouver une crèche”, etc.,
cela renvoie au critère creches.

  * Tu choisis desired_level en fonction de la force du souhait :

    * « si possible, des crèches pas trop loin » (sans insister) → "moyen" voire null si c’est très flou ;

    * « on veut vraiment maximiser nos chances d’avoir une place en crèche », « c’est très important d’avoir une bonne offre de crèches »
→ "assez_eleve" ou "tres_eleve".

    * Si l’utilisateur ne parle pas du tout des crèches, ou dit que ce n’est « pas un sujet » → desired_level: null.

________________
### 5.5. securite
* Si l’utilisateur parle de sécurité, d’insécurité, de “quartier sûr / pas craignos”, de délinquance, etc.,
tu dois activer securite.desired_level.

  * Niveau :

    * « sécurité importante », « on veut un quartier sûr », « éviter les quartiers craignos »
→ souvent "assez_eleve".

    * « très important d’être dans un quartier très sûr », « on est très sensibles à la sécurité », « sécurité prioritaire »
→ "tres_eleve".

    * Si l’utilisateur dit que la sécurité n’est pas vraiment un sujet ou n’y fait jamais référence → desired_level: null.

Tu ne crées jamais de seuil numérique (15/20, etc.) dans le JSON.
Toute notion de seuil chiffré est gérée côté backend.
________________
### 5.6. mediane_rev_decl (revenu médian)
Ce critère reflète le niveau de vie moyen du quartier.
* Si l’utilisateur parle de quartier favorisé, « plutôt aisé », « plutôt bourgeois », « quartiers riches », etc.
→ mediane_rev_decl.desired_level sera "assez_eleve" ou "tres_eleve" selon l’intensité.

* S’il parle plutôt de quartiers « populaires », « mixtes », « pas trop bourgeois », etc.,
tu peux choisir "moyen" ou même "assez_faible" si le discours est très clair (« on veut un quartier populaire »).

* Si l’utilisateur ne parle pas de niveau de vie, ou dit que ce n’est pas un critère → desired_level: null.

________________
### 5.7. part_log_soc (logements sociaux)
* Si l’utilisateur parle de logements sociaux, HLM, « éviter les barres HLM », « on préfère peu de logements sociaux », etc.,
tu actives part_log_soc.

* Par défaut, si quelqu’un mentionne les logements sociaux comme un sujet de préoccupation, on considère généralement qu’il veut plutôt peu de logements sociaux, sauf s’il dit l’inverse.

  * Exemples :

    * « on veut éviter les quartiers avec trop de logements sociaux », « on aimerait une proportion assez faible »
→ "assez_faible" ou "tres_faible".

    * « on veut rester dans un quartier populaire / mixte, ça ne nous dérange pas qu’il y ait des HLM »
→ "moyen" voire "assez_eleve" si l’utilisateur insiste sur le fait que ça ne le gêne pas du tout.

* Si c’est extrêmement ambigu, tu peux laisser desired_level: null.

________________
## 6. Résumé
En résumé, ton travail est :
1. Lire la conversation chat + les infos techniques (zone).

2. Identifier, critère par critère, si :

  * on doit l’activer (desired_level ∈ {tres_faible, assez_faible, moyen, assez_eleve, tres_eleve}),

  * ou le laisser inactif (desired_level: null).

3. Respecter la logique :

  * niveau minimal pour les critères où “plus c’est élevé, mieux c’est” ;

  * niveau maximal pour part_log_soc ;

  * prixMedianM2.max seulement s’il y a une borne explicite en €/m².

4. Ne pas inventer de localisation ni d’autres champs que ceux du JSON.

5. Retourner uniquement l’objet JSON final, bien formé.

FIN DU SYSTEM PROMPT.]
`;

/**  Préfixes d’équipements gérés  ------------------------------- */
const EQUIP_PREFIXES = [
  'boulang',   // boulangerie-pâtisserie
  'bouche',    // commerces de bouche
  'superm',    // super/hypermarchés
  'epicerie',  // épiceries / supérettes
  'lib',       // librairies
  'cinema',    // cinémas
  'conserv',   // conservatoires
  'magbio'     // magasins bio
];

// ----------------------------------
// Charger .env si on n'est pas en production
// ----------------------------------
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// ----------------------------------
// Connexion PG
// ----------------------------------
const pool = new Pool({
  connectionString: process.env.ZENMAP_DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }
    : false
});

const app = express();
app.set('trust proxy', 1);    // Express utilise X-Forwarded-For (Heroku)

// ----------------------------------
// Configuration CORS
// ----------------------------------
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  optionsSuccessStatus: 200
}));

app.use(express.json());

// Anti-scraping — même limite pour tous les endpoints
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,   // 15 min
  max: 1000,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    console.log(`Rate limit atteint pour IP: ${req.ip}`);
    res.status(429).json({ error: 'Too Many Requests' });
  }
});
app.use(limiter);

// ------------------------------------------------------------------
// >>>>>>  UTILITAIRES et FONCTIONS DE FILTRES  <<<<<<
// ------------------------------------------------------------------

// --------------------------------------------------------------
// A) Fonctions utilitaires (intersection, union, différence)
// --------------------------------------------------------------
function intersectArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => setB.has(x));
}
function unionArrays(arrA, arrB) {
  const setA = new Set(arrA);
  for (const x of arrB) {
    setA.add(x);
  }
  return Array.from(setA);
}
function differenceArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => !setB.has(x));
}

// --------------------------------------------------------------
// B) Vérification d'activation des critères
// --------------------------------------------------------------
function isDVFActivated(dvf) {
  if (!dvf) return false;

  if (Array.isArray(dvf.propertyTypes)) {
    dvf.propertyTypes = dvf.propertyTypes.filter(pt => pt != null);
  }

  const hasType = dvf.propertyTypes && dvf.propertyTypes.length > 0;
  const hasBudget = dvf.budget && (
    (dvf.budget.min != null) || (dvf.budget.max != null)
  );
  const hasSurface = dvf.surface && (
    (dvf.surface.min != null) || (dvf.surface.max != null)
  );
  const hasRooms = dvf.rooms && (
    (dvf.rooms.min != null) || (dvf.rooms.max != null)
  );
  const hasYears = dvf.years && (
    (dvf.years.min != null) || (dvf.years.max != null)
  );

  return (hasType || hasBudget || hasSurface || hasRooms || hasYears);
}

function isPrixMedianActivated(pm) {
  if (!pm) return false;
  const hasMin = (pm.min != null && Number(pm.min) > 0); // 0 => n'active PAS
  const hasMax = (pm.max != null);
  return hasMin || hasMax;
}

function isRevenusActivated(rev) {
  if (!rev) return false;
  if (rev.mediane_rev_decl && (rev.mediane_rev_decl.min != null || rev.mediane_rev_decl.max != null)) return true;
  return false;
}
function isLogSocActivated(ls) {
  if (!ls) return false;
  if (ls.part_log_soc && (ls.part_log_soc.min != null || ls.part_log_soc.max != null)) return true;
  return false;
}
function isCollegesActivated(col) {
  if (!col) return false;
  if (col.niveau_coll_min != null || col.niveau_coll_max != null) return true;
  return false;
}
function isEcolesActivated(ec) {
  if (!ec) return false;
  return (
    (ec.ips_min != null || ec.ips_max != null) ||   // filtrage IPS
    ec.rayon != null                                // OU simple rayon
  );
}

// --------------------------------------------------------------
// C) Récupérer communes à partir de départements
// --------------------------------------------------------------
async function getCommunesFromDepartements(depCodes) {
  let allCommunes = [];
  for (let dep of depCodes) {
    const query = `
      SELECT DISTINCT
        CASE
          WHEN (c.insee_com = '75056' OR c.insee_com = '69123' OR c.insee_com = '13055')
               AND c.insee_arm IS NOT NULL AND c.insee_arm <> ''
          THEN c.insee_arm
          ELSE c.insee_com
        END AS commune
      FROM decoupages.communes c
      WHERE c.insee_dep = $1
        AND c.insee_com IS NOT NULL
    `;
    console.time(`getCommunesFromDep-${dep}`);
    let result = await pool.query(query, [dep]);
    console.timeEnd(`getCommunesFromDep-${dep}`);

    let communesDep = result.rows.map(r => r.commune);
    allCommunes.push(...communesDep);
  }
  return Array.from(new Set(allCommunes));
}

async function getArrondissementsForVilleGlobale(codeVille) {
  const sql = `
    SELECT DISTINCT insee_arm
    FROM decoupages.communes
    WHERE insee_com = $1
      AND insee_arm IS NOT NULL
      AND insee_arm <> ''
  `;
  let r = await pool.query(sql, [codeVille]);
  return r.rows.map(row => row.insee_arm);
}

/**
 * Devine si un code correspond à un Département ou une Commune.
 * - Départements : 1–95, 971–974, 976, 2A, 2B…
 * - Communes    : 5 caractères numériques (75056, 35238…)
 */
function looksLikeDepartement(code){
  const s = String(code);
  // Codes numériques 1–95 ou 971–976
  if (/^\d{1,3}$/.test(s))     return true;   // 01, 93, 976…
  // Codes Corse 2A / 2B
  if (/^\d{2}[AB]$/.test(s))   return true;   // 2A, 2B
  return false;
}

async function gatherCommuneCodes(selectedLocalities) {
  let allCodes = [];

  for (let loc of selectedLocalities) {
    /* -----------------------------------------------------------------
       ① Correction automatique : si on reçoit « commune » mais que le
         code ressemble clairement à un département, on corrige.
    ------------------------------------------------------------------*/
    if (loc.type_collectivite === 'commune' && looksLikeDepartement(loc.code_insee)) {
      loc.type_collectivite = 'Département';
    }

    if (loc.type_collectivite === "Département") {
      console.time(`getCommunesFromDep-${loc.code_insee}`);
      let result = await getCommunesFromDepartements([loc.code_insee]);
      console.timeEnd(`getCommunesFromDep-${loc.code_insee}`);
      allCodes.push(...result);
    } else {
      if (loc.type_collectivite === "commune" && ["75056", "69123", "13055"].includes(loc.code_insee)) {
        let arrCodes = await getArrondissementsForVilleGlobale(loc.code_insee);
        allCodes.push(...arrCodes);
      } else {
        allCodes.push(loc.code_insee);
      }
    }
  }

  return Array.from(new Set(allCodes));
}

// --------------------------------------------------------------
// D) Filtrage DVF
// --------------------------------------------------------------
async function getDVFCountTotal(irisList, annee = 2024) {
  if (!irisList.length) return {};

  console.time('getDVFCountTotal');
  const sql = `
    SELECT code_iris, COUNT(*)::int AS nb_total
    FROM dvf_filtre.dvf_simplifie
    WHERE code_iris = ANY($1)
      AND anneemut = $2
    GROUP BY code_iris
  `;
  const res = await pool.query(sql, [irisList, annee]);
  console.timeEnd('getDVFCountTotal');

  const dvfTotalByIris = {};
  for (const row of res.rows) {
    dvfTotalByIris[row.code_iris] = row.nb_total;
  }
  return dvfTotalByIris;
}

async function applyDVF(arrayIrisLoc, dvfCriteria) {
  console.time('D) DVF: activation?');
  if (!isDVFActivated(dvfCriteria)) {
    console.timeEnd('D) DVF: activation?');
    return { irisSet: arrayIrisLoc, dvfCountByIris: {} };
  }
  console.timeEnd('D) DVF: activation?');

  console.time('D) DVF: build query');
  let whereClauses = [];
  let values = [];
  let idx = 1;

  whereClauses.push(`code_iris = ANY($${idx})`);
  values.push(arrayIrisLoc);
  idx++;

  if (dvfCriteria.propertyTypes && dvfCriteria.propertyTypes.length > 0) {
    whereClauses.push(`codtyploc = ANY($${idx})`);
    values.push(dvfCriteria.propertyTypes);
    idx++;
  }
  if (dvfCriteria.budget) {
    if (dvfCriteria.budget.min != null) {
      whereClauses.push(`valeurfonc >= $${idx}`);
      values.push(dvfCriteria.budget.min);
      idx++;
    }
    if (dvfCriteria.budget.max != null) {
      whereClauses.push(`valeurfonc <= $${idx}`);
      values.push(dvfCriteria.budget.max);
      idx++;
    }
  }
  if (dvfCriteria.surface) {
    if (dvfCriteria.surface.min != null) {
      whereClauses.push(`sbati >= $${idx}`);
      values.push(dvfCriteria.surface.min);
      idx++;
    }
    if (dvfCriteria.surface.max != null) {
      whereClauses.push(`sbati <= $${idx}`);
      values.push(dvfCriteria.surface.max);
      idx++;
    }
  }
  if (dvfCriteria.rooms) {
    if (dvfCriteria.rooms.min != null) {
      whereClauses.push(`nbpprinc >= $${idx}`);
      values.push(dvfCriteria.rooms.min);
      idx++;
    }
    if (dvfCriteria.rooms.max != null) {
      whereClauses.push(`nbpprinc <= $${idx}`);
      values.push(dvfCriteria.rooms.max);
      idx++;
    }
  }
  if (dvfCriteria.years) {
    if (dvfCriteria.years.min != null) {
      whereClauses.push(`anneemut >= $${idx}`);
      values.push(dvfCriteria.years.min);
      idx++;
    }
    if (dvfCriteria.years.max != null) {
      whereClauses.push(`anneemut <= $${idx}`);
      values.push(dvfCriteria.years.max);
      idx++;
    }
  }
  console.timeEnd('D) DVF: build query');

  // Modif pour faire le filtre sur l'année
  const annee = 2024; // ou lis-la depuis req.query.annee avec un défaut à 2024
  whereClauses.push(`anneemut = $${values.length + 1}`);
  values.push(annee);

  const wh = `WHERE ` + whereClauses.join(' AND ');
  const query = `
    SELECT code_iris, COUNT(*)::int AS nb_mut
    FROM dvf_filtre.dvf_simplifie
    ${wh}
    GROUP BY code_iris
  `;
  console.time('D) DVF: exec query');
  let res = await pool.query(query, values);
  console.timeEnd('D) DVF: exec query');

  console.log('=> DVF rowCount =', res.rowCount);

  let dvfCountByIris = {};
  let irisOK = [];
  for (let row of res.rows) {
    dvfCountByIris[row.code_iris] = Number(row.nb_mut);
    irisOK.push(row.code_iris);
  }

  console.time('D) DVF: intersection');
  let irisSet = intersectArrays(arrayIrisLoc, irisOK);
  console.timeEnd('D) DVF: intersection');
  console.log('=> after DVF intersectionSet.length =', irisSet.length);

  return { irisSet, dvfCountByIris };
}

// --------------------------------------------------------------
// D) Filtrage DVF bis => prix du mètre carré médian
// --------------------------------------------------------------
async function applyPrixMedian(irisList, pmCriteria) {
  if (!irisList.length) {
    return { irisSet: [], prixMedianByIris: {} };
  }

  let whereClauses = [
    `code_iris = ANY($1)`,
    `periode_prix = '2024-S1'`
  ];
  let vals = [irisList];
  let idx = 2;

  // n'activer l'intersection QUE si le helper le dit
  const activated = isPrixMedianActivated(pmCriteria);

  if (activated) {
    if (pmCriteria?.min != null && Number(pmCriteria.min) > 0) {
      whereClauses.push(`prix_median >= $${idx}`);
      vals.push(pmCriteria.min);
      idx++;
    }
    if (pmCriteria?.max != null) {
      whereClauses.push(`prix_median <= $${idx}`);
      vals.push(pmCriteria.max);
      idx++;
    }
  }

  const sql = `
    SELECT code_iris, prix_median
    FROM dvf_filtre.prix_m2_iris
    WHERE ${whereClauses.join(' AND ')}
  `;
  const result = await pool.query(sql, vals);

  const prixMedianByIris = {};
  const irisOK = [];
  for (const row of result.rows) {
    prixMedianByIris[row.code_iris] = Number(row.prix_median);
    irisOK.push(row.code_iris);
  }

  // 👉 pas d'intersection si "non activé" (cas min=0, max=null)
  const irisSet = activated ? irisList.filter(ci => irisOK.includes(ci)) : irisList;

  return { irisSet, prixMedianByIris };
}

// --------------------------------------------------------------
// E) Filtrage revenus déclarés
// --------------------------------------------------------------
async function applyRevenus(irisList, revCriteria) {
  console.time('E) Revenus: build query');
  
  if (!irisList.length) {
    return { irisSet: [], revenusByIris: {} };
  }

  let whereClauses = [];
  let vals = [];
  let idx = 1;

  whereClauses.push(`code_iris = ANY($${idx})`);
  vals.push(irisList);
  idx++;

  let doIntersection = false;
  if (revCriteria && revCriteria.mediane_rev_decl) {
    if (revCriteria.mediane_rev_decl.min != null) {
      whereClauses.push(`mediane_rev_decl >= $${idx}`);
      vals.push(revCriteria.mediane_rev_decl.min);
      idx++;
      doIntersection = true;
    }
    if (revCriteria.mediane_rev_decl.max != null) {
      whereClauses.push(`mediane_rev_decl <= $${idx}`);
      vals.push(revCriteria.mediane_rev_decl.max);
      idx++;
      doIntersection = true;
    }
  }

  let query = `
    SELECT code_iris, mediane_rev_decl
    FROM filosofi.rev_decl_hl_2021
    WHERE ${whereClauses.join(' AND ')}
  `;
  console.timeEnd('E) Revenus: build query');

  console.time('E) Revenus: exec');
  let r = await pool.query(query, vals);
  console.timeEnd('E) Revenus: exec');
  
  let revenusByIris = {};
  let irisOK = [];
  for (let row of r.rows) {
    let ci = row.code_iris;
    let mv = row.mediane_rev_decl != null ? Number(row.mediane_rev_decl) : null;
    revenusByIris[ci] = { mediane_rev_decl: mv };
    irisOK.push(ci);
  }

  let irisSet;
  if (doIntersection) {
    irisSet = intersectArrays(irisList, irisOK);
  } else {
    irisSet = irisList;
  }

  return { irisSet, revenusByIris };
}

// --------------------------------------------------------------
// F) Filtrage Logements sociaux
// --------------------------------------------------------------
async function applyLogSoc(irisList, lsCriteria) {
  if (!irisList.length) return { irisSet: [], logSocByIris: {} };

  let whereClauses = [ `code_iris = ANY($1)` ];
  let vals = [ irisList ];
  let idx = 2;
  let doIntersection = false;

  if (lsCriteria && lsCriteria.part_log_soc) {
    if (lsCriteria.part_log_soc.min != null) {
      whereClauses.push(`part_log_soc >= $${idx}`);
      vals.push(lsCriteria.part_log_soc.min);
      idx++;
      doIntersection = true;
    }
    if (lsCriteria.part_log_soc.max != null) {
      whereClauses.push(`part_log_soc <= $${idx}`);
      vals.push(lsCriteria.part_log_soc.max);
      idx++;
      doIntersection = true;
    }
  }

  let query = `
    SELECT code_iris, part_log_soc
    FROM filosofi.logsoc_iris_hl_2021
    WHERE ${whereClauses.join(' AND ')}
  `;

  let r = await pool.query(query, vals);

  let logSocByIris = {};
  let irisOK = [];
  for (let row of r.rows) {
    logSocByIris[row.code_iris] = { part_log_soc: Number(row.part_log_soc) };
    irisOK.push(row.code_iris);
  }

  let irisSet = doIntersection
    ? intersectArrays(irisList, irisOK)
    : irisList;

  return { irisSet, logSocByIris };
}

// --------------------------------------------------------------
// G) Filtrage Sécurité (mode rayon) - VERSION DEBUG
// -------------------------------------------------------------
async function applySecurite(irisList, secCrit) {
  if (!irisList.length) {
    return { irisSet: irisList, securiteByIris: {} };
  }

  const { min, max } = secCrit || {};
  const hasFilter = min != null || max != null;

  // Toujours récupérer TOUTES les notes
  const sql = `
    SELECT code_iris, note_sur_20
    FROM delinquance.iris_securite_2023
    WHERE code_iris = ANY($1)
  `;
  const { rows } = await pool.query(sql, [irisList]);

  const securiteByIris = {};
  const irisWithValidNotes = [];
  
  for (const r of rows) {
    const note = r.note_sur_20 !== null ? Number(r.note_sur_20) : null;
    securiteByIris[r.code_iris] = [{ note }];
    
    // Pour le filtrage, on ne garde que ceux qui respectent les bornes
    if (!hasFilter || 
        (note !== null && 
         (min == null || note >= min) && 
         (max == null || note <= max))) {
      irisWithValidNotes.push(r.code_iris);
    }
  }

  // Si pas de filtre actif, on retourne tous les IRIS
  // Si filtre actif, on ne retourne que ceux qui respectent les critères
  const irisSet = hasFilter 
    ? irisList.filter(ci => irisWithValidNotes.includes(ci))
    : irisList;

  return { irisSet, securiteByIris };
}

// --------------------------------------------------------------
// H) Critère partiel Écoles (toujours 300 m par défaut,
//     filtrage IPS/rayon/secteur seulement si l’utilisateur l’active)
// --------------------------------------------------------------
async function applyEcolesRadius(irisList, ec) {
  /* 1.  Valeurs par défaut + détection du filtrage explicite */
  ec = ec || {};

  const rayon   = ec.rayon   ?? 300;          // 300 m si rien n'est précisé
  const ips_min = ec.ips_min ?? null;
  const ips_max = ec.ips_max ?? null;

  // Nettoyer le tableau de secteurs en enlevant les null/undefined
  let secteursArr;
  if (Array.isArray(ec.secteurs)) {
    const cleaned = ec.secteurs.filter(s => s != null && s !== '');
    secteursArr = cleaned.length > 0 ? cleaned : ['PU','PR'];
  } else if (ec.secteur) {
    secteursArr = [ec.secteur];
  } else {
    secteursArr = ['PU','PR'];
  }

  // Détection plus robuste du filtrage actif
  const filteringActive =
    (ips_min !== null || 
     ips_max !== null ||
     (ec.rayon != null && ec.rayon !== 300) ||  // Filtrage seulement si différent de la valeur par défaut
     (Array.isArray(ec.secteurs) && ec.secteurs.filter(s => s != null).length > 0) ||
     ec.secteur != null);

  /* 2.  Construction de la requête */
  let p = 1;
  const vals  = [irisList, rayon, secteursArr];
  const where = [
    `code_iris = ANY($${p++})`,
    `rayon     = $${p++}`,
    `secteur   = ANY($${p++})`
  ];
  if (ips_min !== null) { where.push(`ips >= $${p}`); vals.push(ips_min); p++; }
  if (ips_max !== null) { where.push(`ips <= $${p}`); vals.push(ips_max); p++; }

  const sql = `
    SELECT p.code_iris,
           p.code_rne,
           p.ips,
           p.distance_m,
           g.patronyme_uai,
           g.secteur_public_prive_libe,
           g.adresse_uai,
           g.code_postal_uai,
           g.libelle_commune        AS commune_nom
    FROM   education_ecoles.iris_ecoles_ips_rayon_2025 AS p
    LEFT JOIN   education.geoloc_etab_2025                 AS g
           ON g.numero_uai = p.code_rne
    WHERE  ${where.join(' AND ')}
  `;

  /* 3. Exécution */
  const { rows } = await pool.query(sql, vals);

  /* 4. Agrégation en mémoire */
  const irisOK       = new Set();
  const ecolesByIris = {};

  for (const r of rows) {
    irisOK.add(r.code_iris);
    if (!ecolesByIris[r.code_iris]) ecolesByIris[r.code_iris] = [];
    ecolesByIris[r.code_iris].push({
      code_rne   : r.code_rne,
      ips        : Number(r.ips),
      distance_m : r.distance_m,
      nom        : r.patronyme_uai,
      secteur    : r.secteur_public_prive_libe,
      adresse    : r.adresse_uai,
      cp         : r.code_postal_uai,
      commune    : r.commune_nom
    });
  }

  /* 5. Jeu d'IRIS final */
  let irisSet;
  if (filteringActive) {
    // Si filtrage actif → intersection
    irisSet = Array.from(irisOK);
  } else {
    // Si pas de filtrage → on garde tous les IRIS d'entrée
    irisSet = irisList;
    
    // S'assurer que ecolesByIris contient bien les données pour tous les IRIS
    for (const iris of irisList) {
      if (!ecolesByIris[iris]) {
        ecolesByIris[iris] = [];
      }
    }
  }

  return { irisSet, ecolesByIris };
}

// --------------------------------------------------------------
// I) Critère partiel Collèges
// --------------------------------------------------------------
async function applyColleges(irisList, colCrit) {
  console.time('applyColleges');

  if (!irisList.length) {
    console.timeEnd('applyColleges');
    return {
      irisSet: [],
      collegesByIris: {}
    };
  }

  const DEPS_MANQUANTS = ['17', '22', '2A', '29', '2B', '52', '56'];
  console.time('Colleges coverage');
  const sqlCov = `
    SELECT code_iris, insee_dep
    FROM decoupages.iris_grandeetendue_2022
    WHERE code_iris = ANY($1)
  `;
  let covRes = await pool.query(sqlCov, [irisList]);
  console.timeEnd('Colleges coverage');

  let coverageSet = new Set();
  for (let row of covRes.rows) {
    if (!DEPS_MANQUANTS.includes(row.insee_dep)) {
      coverageSet.add(row.code_iris);
    }
  }

  let subsetCouvert = irisList.filter(ci => coverageSet.has(ci));
  let subsetHors = irisList.filter(ci => !coverageSet.has(ci));

  let collegesByIris = {};
  for (let ci of subsetHors) {
    collegesByIris[ci] = "hors-scope";
  }

  if (!subsetCouvert.length) {
    console.timeEnd('applyColleges');
    return {
      irisSet: subsetHors,
      collegesByIris
    };
  }

  console.time('Colleges pivot');
  let wPivot = [`code_iris = ANY($1)`];
  let vals = [subsetCouvert];
  let idx = 2;

  let doIntersection = false;
  if (colCrit && colCrit.niveau_coll_min != null) {
    wPivot.push(`note_figaro_sur_20 >= $${idx}`);
    vals.push(colCrit.niveau_coll_min);
    idx++;
    doIntersection = true;
  }
  if (colCrit && colCrit.niveau_coll_max != null) {
    wPivot.push(`note_figaro_sur_20 <= $${idx}`);
    vals.push(colCrit.niveau_coll_max);
    idx++;
    doIntersection = true;
  }

const sqlPivot = `
  SELECT p.code_iris,
         p.code_rne,
         g.patronyme_uai,
         p.note_figaro_sur_20,
         g.adresse_uai,
         g.code_postal_uai,
         g.libelle_commune AS commune_nom
  FROM   education_colleges.iris_rne_niveaux_2024 AS p
  LEFT JOIN education.geoloc_etab_2025            AS g
         ON g.numero_uai = p.code_rne
  WHERE  ${wPivot.join(' AND ')}
`;
  let pivotRes = await pool.query(sqlPivot, vals);
  console.timeEnd('Colleges pivot');

  let irisFoundSet = new Set();
  let mapCols = {};
  for (let row of pivotRes.rows) {
    let ci = row.code_iris;
    irisFoundSet.add(ci);
    if (!mapCols[ci]) mapCols[ci] = [];
    mapCols[ci].push({
      code_rne: row.code_rne,
      nom_college: row.patronyme_uai,
      note_sur_20: Number(row.note_figaro_sur_20),
      adresse    : row.adresse_uai,
      cp         : row.code_postal_uai,
      commune    : row.commune_nom
    });
  }

  let finalSet;
  if (doIntersection) {
    finalSet = subsetCouvert.filter(ci => irisFoundSet.has(ci));
  } else {
    finalSet = subsetCouvert;
  }

  for (let ci of finalSet) {
    collegesByIris[ci] = mapCols[ci] || [];
  }

  let irisFinal = finalSet.concat(subsetHors);

  console.log(`applyColleges => coverageRes=${covRes.rowCount} pivotRes=${pivotRes.rowCount}`);
  console.timeEnd('applyColleges');

  return {
    irisSet: irisFinal,
    collegesByIris
  };
}

// --------------------------------------------------------------
// J) Filtrage des crèches, ass mats, tous modes de garde
// --------------------------------------------------------------
function isCrechesActivated(cr) {
  if (!cr) return false;
  return cr.min != null || cr.max != null;
}

function isAssmatsActivated(am) {
  if (!am) return false;
  return am.min != null || am.max != null;
}
function isGardeTotalActivated(gt) {
  if (!gt) return false;
  return gt.min != null || gt.max != null;
}

async function applyCreches(irisList, crechesCrit) {
  if (!irisList.length) return { irisSet: [], crechesByIris: {} };

  // 0. Valeurs par défaut quand l’utilisateur n’a fixé aucune borne
  const { min = null, max = null } = crechesCrit || {};

  const sql = `
    SELECT i.code_iris,
           cr.txcouv_eaje_com
    FROM decoupages.iris_grandeetendue_2022 i
    LEFT JOIN decoupages.communes c
           ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
LEFT JOIN education_creches.tauxcouverture_communes_2022 cr
       ON (cr.numcom = c.insee_com OR cr.numcom = c.insee_arm)
       AND cr.annee = 2022          -- condition déplacée dans le ON pour garder le left-join
WHERE i.code_iris = ANY($1)
      AND ($2::numeric IS NULL OR cr.txcouv_eaje_com IS NULL OR cr.txcouv_eaje_com >= $2)
      AND ($3::numeric IS NULL OR cr.txcouv_eaje_com IS NULL OR cr.txcouv_eaje_com <= $3)
  `;

  const { rows } = await pool.query(sql, [irisList, min, max]);

  const crechesByIris = {};
  const irisOK = [];

  for (const r of rows) {
    crechesByIris[r.code_iris] = r.txcouv_eaje_com != null ? Number(r.txcouv_eaje_com) : null;
    irisOK.push(r.code_iris);
  }

  return {
    irisSet: intersectArrays(irisList, irisOK),
    crechesByIris,
  };
}

// --- Taux couverture Assistantes Maternelles (txcouv_am_ind_com) -----------
async function applyAssmats(irisList, assmatsCrit) {
  if (!irisList.length) return { irisSet: [], assmatsByIris: {} };

  const { min = null, max = null } = assmatsCrit || {};

  const sql = `
    SELECT i.code_iris,
           cr.txcouv_am_ind_com
    FROM decoupages.iris_grandeetendue_2022 i
    LEFT JOIN decoupages.communes c
           ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
    LEFT JOIN education_creches.tauxcouverture_communes_2022 cr
           ON (cr.numcom = c.insee_com OR cr.numcom = c.insee_arm)
          AND cr.annee = 2022
    WHERE i.code_iris = ANY($1)
      AND ($2::numeric IS NULL OR cr.txcouv_am_ind_com IS NULL OR cr.txcouv_am_ind_com >= $2)
      AND ($3::numeric IS NULL OR cr.txcouv_am_ind_com IS NULL OR cr.txcouv_am_ind_com <= $3)
  `;

  const { rows } = await pool.query(sql, [irisList, min, max]);

  const assmatsByIris = {};
  const irisOK = [];

  for (const r of rows) {
    assmatsByIris[r.code_iris] =
      r.txcouv_am_ind_com != null ? Number(r.txcouv_am_ind_com) : null;
    irisOK.push(r.code_iris);
  }

  return { irisSet: intersectArrays(irisList, irisOK), assmatsByIris };
}

// --- Taux couverture TOUS MODES (txcouv_com) -------------------------------
async function applyGardeTotal(irisList, gardeCrit) {
  if (!irisList.length) return { irisSet: [], gardeTotalByIris: {} };

  const { min = null, max = null } = gardeCrit || {};

  const sql = `
    SELECT i.code_iris,
           cr.txcouv_com
    FROM decoupages.iris_grandeetendue_2022 i
    LEFT JOIN decoupages.communes c
           ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
    LEFT JOIN education_creches.tauxcouverture_communes_2022 cr
           ON (cr.numcom = c.insee_com OR cr.numcom = c.insee_arm)
          AND cr.annee = 2022
    WHERE i.code_iris = ANY($1)
      AND ($2::numeric IS NULL OR cr.txcouv_com IS NULL OR cr.txcouv_com >= $2)
      AND ($3::numeric IS NULL OR cr.txcouv_com IS NULL OR cr.txcouv_com <= $3)
  `;

  const { rows } = await pool.query(sql, [irisList, min, max]);

  const gardeTotalByIris = {};
  const irisOK = [];

  for (const r of rows) {
    gardeTotalByIris[r.code_iris] =
      r.txcouv_com != null ? Number(r.txcouv_com) : null;
    irisOK.push(r.code_iris);
  }

  return { irisSet: intersectArrays(irisList, irisOK), gardeTotalByIris };
}

// --------------------------------------------------------------
// K) Filtrage des équipements (par score composite en fonction de la localisation dans et à proximité du quartier)
// --------------------------------------------------------------
/**
 * Filtre la liste d’IRIS sur la base d’un score d’équipement.
 *   - prefix  : 'boulang', 'bouche', … (doit exister dans EQUIP_PREFIXES)
 *   - criteria: {min: <num>|null, max: <num>|null}
 */
async function applyScoreEquip(irisList, prefix, criteria = {}) {
  if (!irisList.length || !EQUIP_PREFIXES.includes(prefix)) {
    return { irisSet: irisList, scoreByIris: {} };
  }

  const { min = null, max = null } = criteria;
  const col = `${prefix}_score`;                       // ex. boulang_score

  const sql = `
    SELECT code_iris, ${col} AS score
    FROM   equipements.iris_equip_2024
    WHERE  code_iris = ANY($1)
      AND ($2::numeric IS NULL OR ${col} >= $2)
      AND ($3::numeric IS NULL OR ${col} <= $3)
  `;

  const { rows } = await pool.query(sql, [irisList, min, max]);

  const scoreByIris = {};
  const keep = new Set();
  for (const r of rows) {
    scoreByIris[r.code_iris] = Number(r.score);
    keep.add(r.code_iris);
  }

  /*  – si aucune borne n’a été fixée → pas d’intersection
      – sinon on conserve uniquement les IRIS qui satisfont la requête   */
  const irisSet = (min == null && max == null)
        ? irisList
        : irisList.filter(ci => keep.has(ci));

  return { irisSet, scoreByIris };
}



// --------------------------------------------------------------
// K) gatherSecuByIris
// --------------------------------------------------------------
async function gatherSecuriteByIris(irisList) {
  if (!irisList.length) {
    return { securiteByIris: {}, irisNameByIris: {} };
  }

  console.time('Securite details: query');
  const q = `
    SELECT i.code_iris,
           i.nom_iris,
           s.note_sur_20
    FROM decoupages.iris_grandeetendue_2022 i
    LEFT JOIN delinquance.iris_securite_2023 s
           ON s.code_iris = i.code_iris
    WHERE i.code_iris = ANY($1)
  `;
  let r = await pool.query(q, [irisList]);
  console.timeEnd('Securite details: query');

  let securiteByIris = {};
  let irisNameByIris = {};
  for (let row of r.rows) {
    let noteValue = (row.note_sur_20 != null) ? Number(row.note_sur_20) : null;
    securiteByIris[row.code_iris] = [{ note: noteValue }];
    irisNameByIris[row.code_iris] = row.nom_iris || '(iris inconnu)';
  }

  return { securiteByIris, irisNameByIris };
}

// --------------------------------------------------------------
// L) groupByCommunes
// --------------------------------------------------------------
async function groupByCommunes(irisList, communesFinal) {
  if (!irisList.length || !communesFinal.length) {
    return [];
  }

  console.time('I) Communes regroupement');
  const query = `
    WITH selected_iris AS (
      SELECT unnest($1::text[]) AS iris
    ),
    expanded AS (
      SELECT s.iris, i.insee_com
      FROM selected_iris s
      JOIN decoupages.iris_grandeetendue_2022 i ON i.code_iris = s.iris
    )
    SELECT e.insee_com, c.nom AS nom_com,
           c.insee_dep, c.nom_dep,
           COUNT(*) AS nb_iris
    FROM expanded e
    JOIN decoupages.communes c
      ON (c.insee_com = e.insee_com OR c.insee_arm = e.insee_com)
    WHERE e.insee_com = ANY($2::text[])
    GROUP BY e.insee_com, c.nom, c.insee_dep, c.nom_dep
    ORDER BY nb_iris DESC
  `;
  let communesRes = await pool.query(query, [irisList, communesFinal]);
  console.timeEnd('I) Communes regroupement');

  console.log('=> Nombre de communes distinctes =', communesRes.rowCount);

  let communesData = communesRes.rows.map(row => ({
    insee_com: row.insee_com,
    nom_com: row.nom_com,
    insee_dep: row.insee_dep,
    nom_dep: row.nom_dep,
    nb_iris: Number(row.nb_iris)
  }));
  return communesData;
}

// ------------------------------------------------------------------
// FONCTION COMMUNE : construit la fiche quartier complète
// ------------------------------------------------------------------
// ------------------------------------------------------------------
// FONCTION COMMUNE : construit la fiche quartier complète
// ------------------------------------------------------------------
async function buildIrisDetail(irisCodes, criteria = {}, equipCriteria = {}) {
  console.time('buildIrisDetail');
  try {
    /* 1️⃣  DVF --------------------------------------------------- */
    const dvfRes          = await applyDVF(irisCodes, criteria?.dvf);
    let   irisCurrent     = dvfRes.irisSet;
    const dvfCountByIris  = dvfRes.dvfCountByIris;
    const dvfTotalByIris  = await getDVFCountTotal(irisCurrent);

    /* 2️⃣  Revenus ---------------------------------------------- */
    const revRes          = await applyRevenus(irisCurrent,  criteria?.filosofi);
    irisCurrent           = revRes.irisSet;
    const revenusByIris   = revRes.revenusByIris;

    /* 3️⃣  Logements sociaux ------------------------------------ */
    const lsRes           = await applyLogSoc(irisCurrent,   criteria?.filosofi);
    irisCurrent           = lsRes.irisSet;
    const logSocByIris    = lsRes.logSocByIris;

    /* 4️⃣  Prix médian m² --------------------------------------- */
    const prixRes         = await applyPrixMedian(irisCurrent, criteria?.prixMedianM2);
    irisCurrent           = prixRes.irisSet;
    const prixMedianByIris= prixRes.prixMedianByIris;

    /* 5️⃣  Écoles (IPS + rayon + secteur) ----------------------- */
    const ecolesRes       = await applyEcolesRadius(irisCurrent, criteria?.ecoles);
    irisCurrent           = ecolesRes.irisSet;
    const ecolesByIris    = ecolesRes.ecolesByIris;

    /* 6️⃣  Collèges --------------------------------------------- */
    const colRes          = await applyColleges(irisCurrent,  criteria?.colleges);
    irisCurrent           = colRes.irisSet;
    const collegesByIris  = colRes.collegesByIris;

    /* 7️⃣  Crèches ---------------------------------------------- */
    const crechesRes      = await applyCreches(irisCurrent,   criteria?.creches);
    irisCurrent           = crechesRes.irisSet;
    const crechesByIris   = crechesRes.crechesByIris;

    /* 7️⃣ bis  Assistantes maternelles (NOUVEAU) ---------------- */
    const amRes           = await applyAssmats(irisCurrent,   criteria?.assmats);
    irisCurrent           = amRes.irisSet;
    const assmatsByIris   = amRes.assmatsByIris;

    /* 7️⃣ ter  Tous modes de garde (NOUVEAU) -------------------- */
    const gtRes           = await applyGardeTotal(irisCurrent, criteria?.garde_total);
    irisCurrent           = gtRes.irisSet;
    const gardeTotalByIris = gtRes.gardeTotalByIris;

    /* 8️⃣  Équipements (scores) ---------------------------------------------- */
    let scoreEquipByIris = {};      // agrège tous les scores demandés

    for (const prefix of EQUIP_PREFIXES) {
      if (!equipCriteria[prefix]) continue;      // pas demandé par l’utilisateur

      const res = await applyScoreEquip(irisCurrent, prefix, equipCriteria[prefix]);
      irisCurrent           = res.irisSet;
      scoreEquipByIris[prefix] = res.scoreByIris;

      // Si plus aucun IRIS ne passe, inutile de poursuivre la boucle
      if (!irisCurrent.length) break;
    }


  /* 8️⃣  Sécurité  ------------ */
  const secRes          = await applySecurite(irisCurrent, criteria?.securite);
  irisCurrent           = secRes.irisSet;          
  const securiteByIris  = secRes.securiteByIris;   // ← contient déjà TOUTES les notes

  /* ➡️  Compléter avec les noms d'IRIS (PAS les notes, on les a déjà) */
  const { irisNameByIris } = await gatherSecuriteByIris(irisCurrent);

    /* 9️⃣  Commune & département -------------------------------- */
    const sqlCom = `
      SELECT i.code_iris,
             COALESCE(NULLIF(c.insee_arm, ''), c.insee_com) AS insee_com,
             c.nom       AS nom_com,
             c.insee_dep AS code_dep,
             d.nom       AS nom_dep
      FROM decoupages.iris_grandeetendue_2022 i
      JOIN decoupages.communes     c ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
      JOIN decoupages.departements d ON c.insee_dep = d.insee_dep
      WHERE i.code_iris = ANY($1)
    `;
    const comRes = await pool.query(sqlCom, [irisCurrent]);

    const communeByIris = {};
    for (const row of comRes.rows) {
      communeByIris[row.code_iris] = {
        nom_commune: row.nom_com,
        code_dep   : row.code_dep,
        nom_dep    : row.nom_dep
      };
    }

    /* 9️⃣ bis  BBOX des IRIS */
    console.time('BBOX query');
const bboxSql = `
  WITH sel AS (SELECT unnest($1::text[]) AS code_iris)
  SELECT sel.code_iris,
         ST_XMin(g) AS west,
         ST_YMin(g) AS south,
         ST_XMax(g) AS east,
         ST_YMax(g) AS north
  FROM sel
  JOIN LATERAL (
    SELECT ST_Transform(geom_2154,4326) AS g
    FROM decoupages.iris_grandeetendue_2022
    WHERE code_iris = sel.code_iris
    LIMIT 1
  ) sub ON true
`;
const { rows: bboxRows } = await pool.query(bboxSql, [irisCurrent]);
console.timeEnd('BBOX query');

const bboxByIris = {};
for (const b of bboxRows) {
  bboxByIris[b.code_iris] = [Number(b.west), Number(b.south),
                             Number(b.east), Number(b.north)];
}


    /* 🔟  Assemblage de la réponse finale ----------------------- */
    const irisFinalDetail = irisCurrent.map(iris => {
      const commune = communeByIris[iris] ?? {};
      const bbox    = bboxByIris[iris]    ?? [null,null,null,null];

      return {
        code_iris        : iris,
        nom_iris         : irisNameByIris[iris]           ?? null,
        commune          : {
          nom_commune : commune.nom_commune ?? null,
          nom_dep     : commune.nom_dep     ?? null,
          code_dep    : commune.code_dep    ?? null
        },
        dvf_count        : dvfCountByIris[iris]           ?? 0,
        dvf_count_total  : dvfTotalByIris[iris]           ?? 0,
        mediane_rev_decl : revenusByIris[iris]?.mediane_rev_decl ?? null,
        part_log_soc     : logSocByIris[iris]?.part_log_soc     ?? null,
        securite         : securiteByIris[iris]?.[0]?.note      ?? null,
        ecoles           : ecolesByIris[iris]             ?? [],
        colleges         : collegesByIris[iris]           ?? [],
        prix_median_m2   : prixMedianByIris[iris]         ?? null,
        taux_creches     : crechesByIris[iris]            ?? null,
        taux_assmats     : assmatsByIris[iris]          ?? null,   // NOUVEAU
        taux_garde_total : gardeTotalByIris[iris]       ?? null,   // NOUVEAU
        score_boulang  : scoreEquipByIris['boulang']?.[iris] ?? null,
        score_bouche   : scoreEquipByIris['bouche']?.[iris]  ?? null,
        score_superm   : scoreEquipByIris['superm']?.[iris]  ?? null,
        score_epicerie : scoreEquipByIris['epicerie']?.[iris]?? null,
        score_lib      : scoreEquipByIris['lib']?.[iris]     ?? null,
        score_cinema   : scoreEquipByIris['cinema']?.[iris]  ?? null,
        score_conserv  : scoreEquipByIris['conserv']?.[iris] ?? null,
        score_magbio   : scoreEquipByIris['magbio']?.[iris]  ?? null,
            bbox_w : bbox[0],
    bbox_s : bbox[1],
    bbox_e : bbox[2],
    bbox_n : bbox[3]
      };
    });

    console.timeEnd('buildIrisDetail');
    return irisFinalDetail;

  } catch (err) {
    console.error('Error in buildIrisDetail:', err);
    console.timeEnd('buildIrisDetail');
    throw err;
  }
}

// ---------------------------
// Helpers pour get_iris_data
// ---------------------------

const RAYONS_ECOLES = [300, 600, 1000, 2000, 5000];
const RAYONS_COMM   = ['in_iris', '300', '600', '1000'];

/** BBox 4326 depuis la table "petiteetendue" (comme l'ancien /iris/:code/bbox) */
async function fetchIrisBbox4326(codeIris) {
  const sql = `
    SELECT
      ST_XMin(geom) AS west,
      ST_YMin(geom) AS south,
      ST_XMax(geom) AS east,
      ST_YMax(geom) AS north
    FROM decoupages.iris_petiteetendue_2022
    WHERE code_iris = $1
    LIMIT 1
  `;
  const { rows } = await pool.query(sql, [codeIris]);
  if (!rows.length) return null;
  const b = rows[0];
  return [Number(b.west), Number(b.south), Number(b.east), Number(b.north)];
}

/** Écoles pour tous les rayons (en 1 requête), sans effet “filtre” */
async function fetchEcolesAllRayons(codeIris) {
  const out = {};
  for (const r of RAYONS_ECOLES) out[String(r)] = [];

  const sql = `
    SELECT p.rayon,
           p.code_rne,
           p.ips,
           p.secteur,
           p.distance_m,
           g.patronyme_uai                 AS nom,
           g.secteur_public_prive_libe     AS secteur_lib,
           g.adresse_uai                   AS adresse,
           g.code_postal_uai               AS cp,
           g.libelle_commune               AS commune
    FROM education_ecoles.iris_ecoles_ips_rayon_2025 p
    JOIN education.geoloc_etab_2025 g
      ON g.numero_uai = p.code_rne
    WHERE p.code_iris = $1
      AND p.rayon     = ANY($2)
    ORDER BY p.rayon, p.distance_m ASC
  `;

  const { rows } = await pool.query(sql, [codeIris, RAYONS_ECOLES]);

  for (const r of rows) {
    const key = String(r.rayon);
    out[key].push({
      rne        : r.code_rne,
      nom        : r.nom,
      // on expose “secteur” tel qu’en base ; si tu préfères le libellé humain :
      secteur    : r.secteur,           // code court (PU/PR si c’est le cas)
      secteur_lib: r.secteur_lib,       // libellé public/privé
      type       : r.type,
      ips        : r.ips != null ? Number(r.ips) : null,
      distance_m : Number(r.distance_m),
      adresse    : r.adresse,
      cp         : r.cp,
      commune    : r.commune
    });
  }

  return out;
}

/** HLM détaillé (tous champs) */
async function fetchHlmDetail(codeIris) {
  const q = `
    SELECT nblspls, part_log_soc, txlsplai, txlsplus, txlspls, txlspli
    FROM filosofi.logsoc_iris_hl_2021
    WHERE code_iris = $1
  `;
  const { rows } = await pool.query(q, [codeIris]);
  if (!rows.length) return null;
  const r = rows[0];
  return {
    nblspls      : r.nblspls      != null ? Number(r.nblspls) : null,
    part_log_soc : r.part_log_soc != null ? Number(r.part_log_soc) / 100 : null,
    txlsplai     : r.txlsplai     != null ? Number(r.txlsplai) / 100     : null,
    txlsplus     : r.txlsplus     != null ? Number(r.txlsplus) / 100     : null,
    txlspls      : r.txlspls      != null ? Number(r.txlspls) / 100      : null,
    txlspli      : r.txlspli      != null ? Number(r.txlspli) / 100      : null
  };
}

/** Commerces : reproduit la même structure que /get_all_commerces */
async function fetchCommercesAll(codeIris) {
  // 1) Liste des préfixes/typequ à partir de equipements.parametres
  const prefixQuery = `
    SELECT equip_prefix, typequ_codes
    FROM equipements.parametres;
  `;
  const { rows: prefixes } = await pool.query(prefixQuery);
  const equipPrefixes = prefixes.map(p => ({ prefix: p.equip_prefix, codes: p.typequ_codes }));

  // 2) Initialiser la structure { prefix: { in_iris:{count,items}, 300:{...}, 600:{...}, 1000:{...} } }
  const commerces = {};
  for (const { prefix } of equipPrefixes) {
    commerces[prefix] = {
      in_iris: { count: 0, items: [] },
      300    : { count: 0, items: [] },
      600    : { count: 0, items: [] },
      1000   : { count: 0, items: [] }
    };
  }
  // S’assure que 'magbio' existe (au cas où pas dans parametres)
  if (!commerces.magbio) {
    commerces.magbio = {
      in_iris: { count: 0, items: [] },
      300    : { count: 0, items: [] },
      600    : { count: 0, items: [] },
      1000   : { count: 0, items: [] }
    };
  }

  // 3) Magasins bio (source dédiée)
  for (const rayon of RAYONS_COMM) {
    let listSql, countSql, params;
    if (rayon === 'in_iris') {
      listSql = `
        SELECT
          TRIM(COALESCE(raison_sociale, '') || ' (' || COALESCE(denomination, '') || ')') AS nom,
          TRIM(COALESCE(addr_lieu::text,'') || ' ' || COALESCE(addr_cp::text,'') || ' ' || COALESCE(addr_ville::text,'')) AS adresse
        FROM equipements.magasins_bio_0725
        WHERE code_iris = $1
          AND cert_etat = 'ENGAGEE'
          AND code_iris IS NOT NULL
        ORDER BY nom
        LIMIT 50;
      `;
      countSql = `
        SELECT COUNT(*) AS total
        FROM equipements.magasins_bio_0725
        WHERE code_iris = $1
          AND cert_etat = 'ENGAGEE'
          AND code_iris IS NOT NULL;
      `;
      params = [codeIris];
    } else {
      const dist = parseInt(rayon, 10);
      listSql = `
        WITH iris_check AS (
          SELECT code_iris, geom_2154
          FROM decoupages.iris_grandeetendue_2022
          WHERE code_iris = $1::text
          LIMIT 1
        )
        SELECT
          TRIM(COALESCE(m.raison_sociale,'') || ' (' || COALESCE(m.denomination,'') || ')') AS nom,
          TRIM(COALESCE(m.addr_lieu::text,'') || ' ' || COALESCE(m.addr_cp::text,'') || ' ' || COALESCE(m.addr_ville::text,'')) AS adresse
        FROM equipements.magasins_bio_0725 m
        CROSS JOIN iris_check i
        WHERE m.cert_etat = 'ENGAGEE'
          AND m.geom_2154 IS NOT NULL
          AND m.code_iris IS NOT NULL
          AND ST_DWithin(m.geom_2154, i.geom_2154, $2)
        ORDER BY nom
        LIMIT 50;
      `;
      countSql = `
        WITH iris_check AS (
          SELECT code_iris, geom_2154
          FROM decoupages.iris_grandeetendue_2022
          WHERE code_iris = $1::text
          LIMIT 1
        )
        SELECT COUNT(*) AS total
        FROM equipements.magasins_bio_0725 m
        CROSS JOIN iris_check i
        WHERE m.cert_etat = 'ENGAGEE'
          AND m.geom_2154 IS NOT NULL
          AND m.code_iris IS NOT NULL
          AND ST_DWithin(m.geom_2154, i.geom_2154, $2);
      `;
      params = [codeIris, dist];
    }
    const { rows: list }   = await pool.query(listSql, params);
    const { rows: counts } = await pool.query(countSql, params);
    commerces.magbio[rayon] = {
      count: parseInt(counts[0]?.total || 0, 10),
      items: list.map(r => ({ nom: r.nom, adresse: r.adresse }))
    };
  }

  // 4) Autres types (BPE 2024)
  const otherPrefixes = equipPrefixes.filter(p => p.prefix !== 'magbio');
  for (const { prefix, codes } of otherPrefixes) {
    for (const rayon of RAYONS_COMM) {
      let listSql, countSql, params;
      if (rayon === 'in_iris') {
        listSql = `
          SELECT
            TRIM(COALESCE(b.nomrs,'') || ' ' || COALESCE(b.cnomrs,'')) AS nom,
            TRIM(
              COALESCE(b.numvoie,'') || ' ' ||
              COALESCE(b.indrep,'')  || ' ' ||
              COALESCE(b.typvoie,'') || ' ' ||
              COALESCE(b.libvoie,'') || ' ' ||
              COALESCE(b.cadr,'')    || ' ' ||
              COALESCE(b.codpos,'')  || ' ' ||
              COALESCE(b.libcom,'')
            ) AS adresse,
            b.typequ_libelle AS type
          FROM equipements.base_2024 b
          WHERE b.code_iris = $1
            AND b.typequ    = ANY($2)
          ORDER BY nom
          LIMIT 50;
        `;
        countSql = `
          SELECT COUNT(*) AS total
          FROM equipements.base_2024
          WHERE code_iris = $1
            AND typequ    = ANY($2);
        `;
        params = [codeIris, codes];
      } else {
        const dist = parseInt(rayon, 10);
          listSql = `
            WITH iris AS (
              SELECT geom_2154
              FROM decoupages.iris_grandeetendue_2022
              WHERE code_iris = $1::text
            )
            SELECT
              TRIM(COALESCE(b.nomrs,'') || ' ' || COALESCE(b.cnomrs,'')) AS nom,
              TRIM(
                COALESCE(b.numvoie,'') || ' ' ||
                COALESCE(b.indrep,'')  || ' ' ||
                COALESCE(b.typvoie,'') || ' ' ||
                COALESCE(b.libvoie,'') || ' ' ||
                COALESCE(b.cadr,'')    || ' ' ||
                COALESCE(b.codpos,'')  || ' ' ||
                COALESCE(b.libcom,'')
              ) AS adresse,
              b.typequ_libelle AS type
            FROM equipements.base_2024 b, iris i
            WHERE b.typequ = ANY($2)
              AND ST_DWithin(b.geom_2154, i.geom_2154, $3)
            ORDER BY nom
            LIMIT 50;
          `;
        countSql = `
          WITH iris AS (
            SELECT geom_2154
            FROM decoupages.iris_grandeetendue_2022
            WHERE code_iris = $1::text
          )
          SELECT COUNT(*) AS total
          FROM equipements.base_2024 b, iris i
          WHERE b.typequ = ANY($2)
            AND ST_DWithin(b.geom_2154, i.geom_2154, $3);
        `;
        params = [codeIris, codes, dist];
      }
      const { rows: list }   = await pool.query(listSql, params);
      const { rows: counts } = await pool.query(countSql, params);

      const items = list.map(r => ({
        nom    : r.nom,
        adresse: r.adresse,
        type   : r.type || null
      }));

      commerces[prefix][rayon] = {
        count: parseInt(counts[0]?.total || 0, 10),
        items
      };
    }
  }

  return commerces;
}

// --- Scores d'équipements en 1 requête ---------------------------------
async function fetchEquipScores(codeIris) {
  const q = `
    SELECT
      boulang_score  AS score_boulang,
      bouche_score   AS score_bouche,
      superm_score   AS score_superm,
      epicerie_score AS score_epicerie,
      lib_score      AS score_lib,
      cinema_score   AS score_cinema,
      conserv_score  AS score_conserv,
      magbio_score   AS score_magbio
    FROM equipements.iris_equip_2024
    WHERE code_iris = $1
    LIMIT 1
  `;
  const { rows } = await pool.query(q, [codeIris]);
  return rows[0] || {};
}

// --- Helper pour appeler le modèle extracteur OpenAI ---
async function callExtractorModel(inputText) {
  const response = await openai.chat.completions.create({
    model: "gpt-4.1-mini",
    messages: [
      { role: "system", content: EXTRACTOR_SYSTEM_PROMPT },
      { role: "user", content: inputText },
    ],
  });

  const assistantMessage = response.choices[0].message.content;

  let json;
  try {
    json = JSON.parse(assistantMessage);
  } catch (err) {
    console.error("Erreur de parsing JSON de l'assistant extracteur :", err);
    console.error("Texte brut :", assistantMessage);
    throw new Error("INVALID_JSON_FROM_EXTRACTOR");
  }

  return json;
}

// --- Helper Zenmap AI : construit l'input et appelle l'assistant extracteur ---
async function runZenmapExtractor(zone_recherche, chat_transcript) {
  const zoneBlock = zone_recherche
    ? JSON.stringify(zone_recherche, null, 2)
    : "null";

  const inputText = `
[ZONE_RECHERCHE]
${zoneBlock}
---
[TRANSCRIPT]
${chat_transcript}
---
[INSTRUCTIONS SUPPLÉMENTAIRES]
- Ton output doit être du JSON valide uniquement, sans texte autour (pas d'explications).
- Respecte strictement le schéma demandé dans le prompt système.
`.trim();

  // On réutilise le helper générique qui gère l'appel OpenAI + le JSON.parse
  const json = await callExtractorModel(inputText);
  return json;
}

// --------------------------------------------------------------
// MATCHING V1 avec bornes Jenks en dur
// --------------------------------------------------------------

// ⚠ IMPORTANT : ces labels doivent correspondre EXACTEMENT
// à ce que tu as mis dans le prompt de l'assistant extracteur.
const LEVELS = ["tres_faible", "assez_faible", "moyen", "assez_eleve", "tres_eleve"];

// Bornes Jenks en dur par critère.
// ➜ À ADAPTER avec TES vraies valeurs.
const JENKS_BOUNDS = {
  // Exemple fictif pour revenus déclarés (en euros/an)
  mediane_rev_decl: {
    tres_faible: { min: 0,      max: 15000 },
    assez_faible:      { min: 15000,  max: 22000 },
    moyen:       { min: 22000,  max: 28000 },
    assez_eleve:       { min: 28000,  max: 35000 },
    tres_eleve:  { min: 35000,  max: 100000 }
  },

  // Exemple fictif pour part de logements sociaux (ratio 0–1)
  part_log_soc: {
    tres_faible: { min: 0.0,  max: 0.05 },
    assez_faible:      { min: 0.05, max: 0.15 },
    moyen:       { min: 0.15, max: 0.30 },
    assez_eleve:       { min: 0.30, max: 0.50 },
    tres_eleve:  { min: 0.50, max: 1.00 }
  },

  // Exemple fictif pour sécurité (note sur 20)
  securite: {
    tres_faible: { min: 0,   max: 8 },   // quartiers très peu sûrs
    assez_faible:      { min: 8,   max: 12 },
    moyen:       { min: 12,  max: 15 },
    assez_eleve:       { min: 15,  max: 17 },
    tres_eleve:  { min: 17,  max: 20 }   // quartiers les plus sûrs
  }

  // IPS des écoles primaires dans iris_ecoles_rayon
    // (échelles à adapter à tes Jenks réels)
    ecoles: {
      tres_faible:  { min:  60, max:  85 },  // IPS très faibles
      assez_faible: { min:  85, max:  95 },
      moyen:        { min:  95, max: 105 },
      assez_eleve:  { min: 105, max: 115 },
      tres_eleve:   { min: 115, max: 140 }   // très bons IPS
    },

  // Note des collèges (par ex. note Figaro sur 20)
  colleges: {
    tres_faible:  { min:  0,  max:  8 },
    assez_faible: { min:  8,  max: 11 },
    moyen:        { min: 11,  max: 13 },
    assez_eleve:  { min: 13,  max: 15 },
    tres_eleve:   { min: 15,  max: 20 }
  },

  // Taux de couverture crèches (txcouv_eaje_com), en %
  creches: {
    tres_faible:  { min:   0, max:  20 },
    assez_faible: { min:  20, max:  40 },
    moyen:        { min:  40, max:  60 },
    assez_eleve:  { min:  60, max:  80 },
    tres_eleve:   { min:  80, max: 120 }   // >100% possible selon la méthode
  }
};

// Récupère les bornes pour un critère donné
function getBoundsForCriterion(critKey) {
  return JENKS_BOUNDS[critKey] || null;
}

// Min / max global pour un critère à partir des bornes Jenks
function getGlobalMinMax(bounds) {
  if (!bounds) return { globalMin: null, globalMax: null };
  const levels = Object.keys(bounds);
  if (!levels.length) return { globalMin: null, globalMax: null };

  let globalMin = Infinity;
  let globalMax = -Infinity;

  for (const lvl of levels) {
    const b = bounds[lvl];
    if (!b) continue;
    if (b.min != null && b.min < globalMin) globalMin = b.min;
    if (b.max != null && b.max > globalMax) globalMax = b.max;
  }

  if (!Number.isFinite(globalMin) || !Number.isFinite(globalMax)) {
    return { globalMin: null, globalMax: null };
  }

  return { globalMin, globalMax };
}

// Score pour "higher_better"
function scoreHigherBetter(value, desired_level, bounds) {
  if (value == null || !bounds) return 0;

  const { globalMin, globalMax } = getGlobalMinMax(bounds);
  if (globalMin == null || globalMax == null || globalMax === globalMin) return 0;

  // Plateau à 0 tout en bas
  if (value <= globalMin) return 0;

  const topStart = bounds.tres_eleve.min; // début du plateau à 100 %
  const userBounds = bounds[desired_level] || bounds.moyen;
  const userMin = userBounds.min;

  // Au-dessus du début de "tres_eleve" → 1
  if (value >= topStart) return 1;

  // Entre globalMin et userMin → 0 -> 0.5
  if (value < userMin) {
    const denom = (userMin - globalMin) || 1e-9;
    return 0.5 * (value - globalMin) / denom;
  }

  // Entre userMin et topStart → 0.5 -> 1
  const denom = (topStart - userMin) || 1e-9;
  return 0.5 + 0.5 * (value - userMin) / denom;
}

// Score pour "lower_better"
function scoreLowerBetter(value, desired_level, bounds) {
  if (value == null || !bounds) return 0;

  const { globalMin, globalMax } = getGlobalMinMax(bounds);
  if (globalMin == null || globalMax == null || globalMax === globalMin) return 0;

  // Tout en haut → 0
  if (value >= globalMax) return 0;

  const bestMax = bounds.tres_faible.max; // haut de la zone "très faible"
  const userBounds = bounds[desired_level] || bounds.moyen;
  const userMax = userBounds.max;

  // Très faible (<= bestMax) → 1
  if (value <= bestMax) return 1;

  // Entre bestMax et userMax → 1 -> 0.5
  if (value <= userMax) {
    const denom = (userMax - bestMax) || 1e-9;
    return 0.5 + 0.5 * (userMax - value) / denom;
  }

  // Au-dessus de userMax → 0.5 -> 0
  const denom = (globalMax - userMax) || 1e-9;
  return 0.5 * (globalMax - value) / denom;
}

// Score pour "target_band" (on vise la bande du desired_level)
function scoreTargetBand(value, desired_level, bounds) {
  if (value == null || !bounds) return 0;

  const { globalMin, globalMax } = getGlobalMinMax(bounds);
  if (globalMin == null || globalMax == null || globalMax === globalMin) return 0;

  const band = bounds[desired_level] || bounds.moyen;
  const bandMin = band.min;
  const bandMax = band.max;

  // Dans la bande cible → 1
  if (value >= bandMin && value <= bandMax) return 1;

  // En dessous → 0 -> 0.5
  if (value < bandMin) {
    const denom = (bandMin - globalMin) || 1e-9;
    return 0.5 * (value - globalMin) / denom;
  }

  // Au-dessus → 0.5 -> 0
  const denom = (globalMax - bandMax) || 1e-9;
  return 0.5 * (globalMax - value) / denom;
}

// Récupère les IRIS de la zone (collectivites ou radius)
async function getIrisFromZone(zone_recherche) {
  const { mode, collectivites = [], radius_center, radius_km } = zone_recherche || {};
  let irisList = [];

  if (mode === 'collectivites') {
    // On réutilise ta logique : looksLikeDepartement + gatherCommuneCodes
    let selectedLocalities;

    if (collectivites.length && typeof collectivites[0] === 'string') {
      selectedLocalities = collectivites.map(code => ({
        code_insee: String(code),
        type_collectivite: looksLikeDepartement(code) ? 'Département' : 'commune'
      }));
    } else {
      selectedLocalities = collectivites.map(loc => ({
        code_insee: String(loc.code_insee || loc.code || loc.insee_com),
        type_collectivite:
          loc.type_collectivite
          || (looksLikeDepartement(loc.code_insee || loc.code || loc.insee_com)
              ? 'Département'
              : 'commune')
      }));
    }

    const communesFinal = await gatherCommuneCodes(selectedLocalities);
    if (communesFinal.length) {
      const sql = `
        SELECT code_iris
        FROM decoupages.iris_grandeetendue_2022
        WHERE insee_com = ANY($1)
      `;
      const { rows } = await pool.query(sql, [communesFinal]);
      irisList = rows.map(r => r.code_iris);
    }

  } else if (mode === 'radius') {
    if (!radius_center || radius_center.lon == null || radius_center.lat == null || !radius_km) {
      return [];
    }
    const radius_m = Number(radius_km) * 1000;
    const sql = `
      SELECT code_iris
      FROM decoupages.iris_grandeetendue_2022
      WHERE ST_DWithin(
        geom_2154,
        ST_Transform(ST_SetSRID(ST_MakePoint($1,$2),4326),2154),
        $3
      )
    `;
    const { rows } = await pool.query(sql, [radius_center.lon, radius_center.lat, radius_m]);
    irisList = rows.map(r => r.code_iris);
  }

  return irisList;
}

// Calcul complet du matching V1 (budget + revenus + log_soc + sécurité)
async function computeMatching(zone_recherche, criteria) {
  // 1) Récupérer tous les IRIS de la zone
  const irisList = await getIrisFromZone(zone_recherche);
  if (!irisList.length) {
    return [];
  }

  // 2) Hydratation des indicateurs
  const ecolesCriteria = criteria.ecoles || {};

  // Rayon par défaut si non fourni par l’assistant extracteur :
  // 600 m ≈ 10 minutes à pied
  let rayonEcoles = ecolesCriteria.rayon;
  if (rayonEcoles == null) {
    rayonEcoles = 600;
  }

  const [
    revRes,
    logRes,
    secRes,
    prixRes,
    ecolesRes,
    collegesRes,
    crechesRes
  ] = await Promise.all([
    applyRevenus(irisList, null),
    applyLogSoc(irisList, null),
    applySecurite(irisList, null),
    applyPrixMedian(irisList, null),
    applyEcolesRadius(irisList, {
      ...ecolesCriteria,
      rayon: rayonEcoles
    }),
    applyColleges(irisList, null),
    applyCreches(irisList, null)
  ]);

  const revenusByIris     = revRes.revenusByIris        || {};
  const logSocByIris      = logRes.logSocByIris         || {};
  const securiteByIris    = secRes.securiteByIris       || {};
  const prixMedianByIris  = prixRes.prixMedianByIris    || {};
  const ecolesByIris      = (ecolesRes && ecolesRes.ecolesByIris)       || {};
  const collegesByIris    = (collegesRes && collegesRes.collegesByIris) || {};
  const crechesByIris     = (crechesRes && crechesRes.crechesByIris)    || {};

  // 3) Agrégation pour écoles / collèges / crèches

  // Meilleur IPS d'école primaire par IRIS (dans le rayon / secteurs choisis)
  const bestEcoleIpsByIris = {};
  for (const iris of irisList) {
    const list = ecolesByIris[iris] || [];
    let best = null;

    for (const e of list) {
      if (e.ips == null) continue;
      const v = Number(e.ips);
      if (!Number.isFinite(v)) continue;
      if (best == null || v > best) {
        best = v;
      }
    }

    if (best != null) {
      bestEcoleIpsByIris[iris] = best;
    }
  }

  // Meilleure note de collège par IRIS
  const bestCollegeNoteByIris = {};
  for (const iris of irisList) {
    const list = collegesByIris[iris] || [];
    let best = null;

    for (const c of list) {
      if (c.note_sur_20 == null) continue;
      const v = Number(c.note_sur_20);
      if (!Number.isFinite(v)) continue;
      if (best == null || v > best) {
        best = v;
      }
    }

    if (best != null) {
      bestCollegeNoteByIris[iris] = best;
    }
  }

  // Crèches : une valeur unique par IRIS (taux couverture)
  // crechesByIris[iris] est déjà un nombre ou null.

  // 4) Préparation des critères actifs (desired_level + direction + bornes Jenks)

  const activeCriteriaConfigs = [];

  function registerLevelCriterion(critKey, getValueFn) {
    const crit = criteria[critKey];
    if (!crit) return;

    const { desired_level, direction } = crit;
    if (!desired_level || !direction) return;
    if (!LEVELS.includes(desired_level)) return;

    const bounds = getBoundsForCriterion(critKey);
    if (!bounds) return;

    activeCriteriaConfigs.push({
      critKey,
      desired_level,
      direction,
      getValue: getValueFn,
      bounds
    });
  }

  // Sécurité (note sur 20)
  registerLevelCriterion('securite', (iris) => {
    const arr = securiteByIris[iris];
    if (!arr || !arr.length) return null;
    return arr[0].note ?? null;
  });

  // Revenus déclarés
  registerLevelCriterion('mediane_rev_decl', (iris) => {
    const obj = revenusByIris[iris];
    return obj ? obj.mediane_rev_decl : null;
  });

  // Logements sociaux (part_log_soc, ratio 0–1)
  registerLevelCriterion('part_log_soc', (iris) => {
    const obj = logSocByIris[iris];
    return obj ? obj.part_log_soc : null;
  });

  // Écoles primaires (IPS max dans le rayon)
  registerLevelCriterion('ecoles', (iris) => {
    const v = bestEcoleIpsByIris[iris];
    return v != null ? v : null;
  });

  // Collèges (meilleure note sur 20)
  registerLevelCriterion('colleges', (iris) => {
    const v = bestCollegeNoteByIris[iris];
    return v != null ? v : null;
  });

  // Crèches (taux de couverture)
  registerLevelCriterion('creches', (iris) => {
    const v = crechesByIris[iris];
    return v != null ? v : null;
  });

  // Budget (prix median m2) – traitement spécifique
  const budgetCrit = criteria.prixMedianM2 || null;

  // 5) Calcul du score pour chaque IRIS
  const matches = [];

  for (const iris of irisList) {
    const perCriterion = {};
    let sumScores = 0;
    let countScores = 0;

    // a) Critères de type niveau (desired_level + direction + bornes Jenks)
    for (const cfg of activeCriteriaConfigs) {
      const v = cfg.getValue(iris);
      let s = 0;

      if (v == null || Number.isNaN(v)) {
        s = 0;
      } else if (cfg.direction === 'higher_better') {
        s = scoreHigherBetter(v, cfg.desired_level, cfg.bounds);
      } else if (cfg.direction === 'lower_better') {
        s = scoreLowerBetter(v, cfg.desired_level, cfg.bounds);
      } else if (cfg.direction === 'target_band') {
        s = scoreTargetBand(v, cfg.desired_level, cfg.bounds);
      } else {
        s = 0;
      }

      perCriterion[cfg.critKey] = { value: v, score: s };
      sumScores += s;
      countScores += 1;
    }

    // b) Budget – prix médian m² vs budget max
    if (budgetCrit && budgetCrit.max != null) {
      const maxBudget = Number(budgetCrit.max);
      const prix = prixMedianByIris[iris] ?? null;
      let s = 0;

      if (prix == null || Number.isNaN(prix)) {
        s = 0;
      } else if (prix <= maxBudget) {
        s = 1;
      } else {
        const ratio = prix / maxBudget;
        if (ratio <= 1.3) {
          s = 1 - (ratio - 1) / 0.3; // 100% → 0% entre 100% et 130% du budget
        } else {
          s = 0;
        }
      }

      perCriterion.prixMedianM2 = { value: prix, score: s };
      sumScores += s;
      countScores += 1;
    }

    const globalScore = countScores ? (sumScores / countScores) : 0;

    matches.push({
      code_iris: iris,
      score: globalScore,
      scores: perCriterion
    });
  }

  // 6) Tri décroissant par score global
  matches.sort((a, b) => b.score - a.score);

  return matches;
}

// ------------------------------------------------------------------
// POST /get_iris_filtre  (version LITE : rapide, sans hydratation)
// ------------------------------------------------------------------
app.post('/get_iris_filtre', async (req, res) => {
  console.log('>>> BODY RECEIVED FROM BUBBLE:', JSON.stringify(req.body, null, 2));
  console.time('TOTAL /get_iris_filtre_lite');

  try {
    const { mode, codes_insee, center, radius_km, criteria = {}, iris_base } = req.body;

    // 0) Récupération de la liste initiale d’IRIS (localisation)
    let irisSet = [];

    // Bypass : si Bubble envoie déjà la base d’IRIS
    if (Array.isArray(iris_base) && iris_base.length) {
      console.log(`🔄 Bypass localisation : ${iris_base.length} IRIS reçus`);
      irisSet = iris_base.map(String);

    } else if (mode === 'collectivites') {
      // Convertir départements/communes en codes communes finaux
      const selectedLocalities = (codes_insee || []).map(code => ({
        code_insee: code,
        type_collectivite: looksLikeDepartement(code) ? 'Département' : 'commune'
      }));
      const communesFinal = await gatherCommuneCodes(selectedLocalities);

      if (communesFinal.length) {
        const sql = `
          SELECT code_iris
          FROM decoupages.iris_grandeetendue_2022
          WHERE insee_com = ANY($1)
        `;
        const { rows } = await pool.query(sql, [communesFinal]);
        irisSet = rows.map(r => r.code_iris);
      }

    } else if (mode === 'rayon') {
      if (!center || center.lon == null || center.lat == null) {
        console.timeEnd('TOTAL /get_iris_filtre_lite');
        return res.status(400).json({ error: 'lon and lat are required for rayon mode' });
      }
      const radius_m = Number(radius_km) * 1000;
      const sql = `
        SELECT code_iris
        FROM decoupages.iris_grandeetendue_2022
        WHERE ST_DWithin(
          geom_2154,
          ST_Transform(ST_SetSRID(ST_MakePoint($1,$2),4326),2154),
          $3
        )
      `;
      const { rows } = await pool.query(sql, [center.lon, center.lat, radius_m]);
      irisSet = rows.map(r => r.code_iris);

    } else {
      console.timeEnd('TOTAL /get_iris_filtre_lite');
      return res.status(400).json({ error: 'mode invalid' });
    }

    if (!irisSet.length) {
      console.timeEnd('TOTAL /get_iris_filtre_lite');
      return res.json({ nb_iris: 0, iris: [] });
    }

    // 1) Application des critères SANS hydratation (on ne garde que le set d’IRIS)
    const applyIf = async (fn, active, ...args) => active ? (await fn(...args)).irisSet : args[0];

    // DVF
    irisSet = await applyIf(applyDVF, isDVFActivated(criteria?.dvf), irisSet, criteria.dvf);

    // Revenus / niveau de vie
    irisSet = await applyIf(applyRevenus, isRevenusActivated(criteria?.filosofi), irisSet, criteria.filosofi);

    // Logements sociaux (si critère utilisé)
    irisSet = await applyIf(applyLogSoc, isLogSocActivated(criteria?.filosofi), irisSet, criteria.filosofi);

    // Prix médian m² (n'active pas pour min=0)
    if (isPrixMedianActivated(criteria?.prixMedianM2)) {
      irisSet = (await applyPrixMedian(irisSet, criteria.prixMedianM2)).irisSet;
    }

    // Écoles
    irisSet = await applyIf(applyEcolesRadius, isEcolesActivated(criteria?.ecoles), irisSet, criteria.ecoles);

    // Collèges
    irisSet = await applyIf(applyColleges, isCollegesActivated(criteria?.colleges), irisSet, criteria.colleges);

    // Crèches
    irisSet = await applyIf(applyCreches, isCrechesActivated(criteria?.creches), irisSet, criteria.creches);

    // Assistantes maternelles (NOUVEAU)
    irisSet = await applyIf(applyAssmats, isAssmatsActivated(criteria?.assmats), irisSet, criteria.assmats);

    // Tous modes de garde confondus (NOUVEAU)
    irisSet = await applyIf(applyGardeTotal, isGardeTotalActivated(criteria?.garde_total), irisSet, criteria.garde_total);

    // Équipements (scores BPE)
    if (criteria?.equipements) {
      for (const prefix of EQUIP_PREFIXES) {
        if (criteria.equipements[prefix]) {
          irisSet = (await applyScoreEquip(irisSet, prefix, criteria.equipements[prefix])).irisSet;
          if (!irisSet.length) break;
        }
      }
    }

    // Sécurité
    if (criteria?.securite) {
      const secRes = await applySecurite(irisSet, criteria.securite);
      irisSet = secRes.irisSet;
    }

    if (!irisSet.length) {
      console.timeEnd('TOTAL /get_iris_filtre_lite');
      return res.json({ nb_iris: 0, iris: [] });
    }

// 2) Récupération LÉGÈRE des noms d’IRIS ET du nom de commune
const nameSql = `
  SELECT i.code_iris,
         i.nom_iris,
         c.nom AS nom_commune
  FROM decoupages.iris_grandeetendue_2022 i
  LEFT JOIN LATERAL (
    SELECT nom
    FROM decoupages.communes c
    WHERE c.insee_com = i.insee_com OR c.insee_arm = i.insee_com
    LIMIT 1
  ) c ON true
  WHERE i.code_iris = ANY($1)
  ORDER BY array_position($1::text[], i.code_iris)
`;
const { rows: r2 } = await pool.query(nameSql, [irisSet]);

const iris = r2.map(r => ({
  code_iris: r.code_iris,
  nom_iris: r.nom_iris,
  nom_commune: r.nom_commune || null
}));

console.timeEnd('TOTAL /get_iris_filtre_lite');
return res.json({ nb_iris: iris.length, iris });

  } catch (err) {
    console.error('Erreur /get_iris_filtre (lite):', err);
    console.timeEnd('TOTAL /get_iris_filtre_lite');
    return res.status(500).json({ error: 'server', details: err.message });
  }
});

// ------------------------------------------------------------------
// GET /iris/:code/bbox           (table iris_petiteetendue_2022, SRID 4326)
// ------------------------------------------------------------------
app.get('/iris/:code/bbox', async (req, res) => {
  const { code } = req.params;
  if (!code) return res.status(400).json({ error: 'Code IRIS requis' });

  const sql = `
    SELECT
      ST_XMin(geom) AS west,
      ST_YMin(geom) AS south,
      ST_XMax(geom) AS east,
      ST_YMax(geom) AS north,
      nom_iris
    FROM decoupages.iris_petiteetendue_2022
    WHERE code_iris = $1
    LIMIT 1
  `;

  try {
    const { rows } = await pool.query(sql, [code]);
    if (!rows.length) return res.status(404).json({ error: 'IRIS non trouvé' });

    const b = rows[0];
    res.json({
      code_iris: code,
      nom_iris : b.nom_iris,
      bbox     : [Number(b.west), Number(b.south), Number(b.east), Number(b.north)]
    });
  } catch (err) {
    console.error('Erreur /iris/:code/bbox :', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// ------------------------------------------------------------------
// POST /get_iris_data   (renvoie TOUT pour 1 IRIS)
// Body attendu: { code_iris: "XXXXXXXXX" }  (toujours 1 code)
// ------------------------------------------------------------------
app.post('/get_iris_data', async (req, res) => {
  try {
    const code = String(req.body?.code_iris || '').trim();
    if (!code) return res.status(400).json({ error: 'code_iris requis' });

    // 1) Base "fiche" via ta logique existante (DVF, revenus, part_log_soc, sécurité, prix, crèches, scores, etc.)
    //    NB: on passe un tableau [code] à buildIrisDetail, on récupère le 1er (et seul) objet.
    const baseArr = await buildIrisDetail([code], /*criteria*/ {}, /*equipCriteria*/ {});
    const base = (Array.isArray(baseArr) && baseArr[0]) ? baseArr[0] : null;
    if (!base) return res.status(404).json({ error: 'IRIS non trouvé' });

    // 2) BBox 4326 depuis "iris_petiteetendue_2022" (écrase la bbox éventuelle)
    const bbox4326 = await fetchIrisBbox4326(code);

    // 3) HLM détaillé (tous champs)
    const hlm = await fetchHlmDetail(code);

    // 4) Écoles (multi-rayons)
    const ecoles = await fetchEcolesAllRayons(code);

    // 5) Commerces (même structure que /get_all_commerces)
    const commerces = await fetchCommercesAll(code);

    // 👉 NOUVEAU : 5bis) Scores équipements en 1 requête
    const equipScores = await fetchEquipScores(code);

    // 6) Assemblage final (1 seul objet, pas de nb_iris, pas de centroid)
    const out = {
      code_iris       : base.code_iris,
      nom_iris        : base.nom_iris,
      commune         : base.commune,           // {nom_commune, nom_dep, code_dep}
      dvf_count       : base.dvf_count ?? 0,
      dvf_count_total : base.dvf_count_total ?? 0,
      mediane_rev_decl: base.mediane_rev_decl ?? null,
      part_log_soc    : base.part_log_soc ?? null,    // conserve le champ synthétique
      securite        : base.securite ?? null,
      prix_median_m2  : base.prix_median_m2 ?? null,
      taux_creches    : base.taux_creches ?? null,
      taux_assmats     : base.taux_assmats ?? null,
      taux_garde_total : base.taux_garde_total ?? null,
      // 🔁 Scores d'équipements (1 requête)
      score_boulang   : equipScores.score_boulang  ?? base.score_boulang  ?? null,
      score_bouche    : equipScores.score_bouche   ?? base.score_bouche   ?? null,
      score_superm    : equipScores.score_superm   ?? base.score_superm   ?? null,
      score_epicerie  : equipScores.score_epicerie ?? base.score_epicerie ?? null,
      score_lib       : equipScores.score_lib      ?? base.score_lib      ?? null,
      score_cinema    : equipScores.score_cinema   ?? base.score_cinema   ?? null,
      score_conserv   : equipScores.score_conserv  ?? base.score_conserv  ?? null,
      score_magbio    : equipScores.score_magbio   ?? base.score_magbio   ?? null,
      // BBox depuis petiteetendue
      bbox            : bbox4326 || [null, null, null, null],
      // Ajouts détaillés
      hlm,            // { nblspls, part_log_soc, txlsplai, txlsplus, txlspls, txlspli }
      ecoles,         // { "300":[...], "600":[...], "1000":[...], "2000":[...], "5000":[...] }
      commerces,       // { prefix: { in_iris:{count,items}, 300:{...}, 600:{...}, 1000:{...} }, magbio:{...} }
      colleges        : (Array.isArray(base.colleges) || base.colleges === 'hors-scope')
                        ? base.colleges
                        : []
    };

    return res.json(out);

  } catch (err) {
    console.error('Erreur /get_iris_data :', err);
    return res.status(500).json({ error: 'Erreur serveur', details: err.message });
  }
});

// ------------------------------------------------------------------
// GET /iris_by_point  (version LITE : rapide, sans hydratation)
// Params : lat, lon (obligatoires), radius_km (optionnel, défaut 0.3)
// Retour : { nb_iris, iris: [ { code_iris, nom_iris, nom_commune } ] }
// ------------------------------------------------------------------
app.get('/iris_by_point', async (req, res) => {
  console.time('TOTAL /iris_by_point_lite');
  const { lat, lon, radius_km = '0.3' } = req.query;

  if (lat == null || lon == null) {
    console.timeEnd('TOTAL /iris_by_point_lite');
    return res.status(400).json({ error: 'lat & lon are required' });
  }

  try {
    const radius_m = Number(radius_km) * 1000;

    // A) IRIS cible = celui qui contient le point
    const cibleSql = `
      SELECT code_iris
      FROM decoupages.iris_grandeetendue_2022
      WHERE ST_Contains(
              geom_2154,
              ST_Transform(ST_SetSRID(ST_MakePoint($1,$2),4326),2154)
            )
      LIMIT 1
    `;
    const cibleRes = await pool.query(cibleSql, [lon, lat]);
    if (!cibleRes.rows.length) {
      console.timeEnd('TOTAL /iris_by_point_lite');
      return res.status(404).json({ error: 'IRIS not found' });
    }
    const codeCible = cibleRes.rows[0].code_iris;

    // B) IRIS voisins = ceux qui coupent le disque de rayon radius_m
    const voisinsSql = `
      SELECT i.code_iris
      FROM decoupages.iris_grandeetendue_2022 i
      WHERE ST_DWithin(
              i.geom_2154,
              ST_Transform(ST_SetSRID(ST_MakePoint($1,$2),4326),2154),
              $3
            )
    `;
    const vRes = await pool.query(voisinsSql, [lon, lat, radius_m]);
    let irisList = vRes.rows.map(r => r.code_iris);

    // S'assurer que l'IRIS cible est présent
    if (!irisList.includes(codeCible)) irisList.unshift(codeCible);

    if (!irisList.length) {
      console.timeEnd('TOTAL /iris_by_point_lite');
      return res.json({ nb_iris: 0, iris: [] });
    }

    // C) Récupération légère des noms d’IRIS + nom commune (comme /get_iris_filtre lite)
    const nameSql = `
      SELECT i.code_iris,
             i.nom_iris,
             c.nom AS nom_commune
      FROM decoupages.iris_grandeetendue_2022 i
      LEFT JOIN LATERAL (
        SELECT nom
        FROM decoupages.communes c
        WHERE c.insee_com = i.insee_com OR c.insee_arm = i.insee_com
        LIMIT 1
      ) c ON true
      WHERE i.code_iris = ANY($1)
      ORDER BY
        CASE WHEN i.code_iris = $2 THEN 0 ELSE 1 END,   -- cible en premier
        array_position($1::text[], i.code_iris)          -- puis l'ordre d'origine
    `;
    const { rows: rowsNames } = await pool.query(nameSql, [irisList, codeCible]);

    const iris = rowsNames.map(r => ({
      code_iris: r.code_iris,
      nom_iris: r.nom_iris,
      nom_commune: r.nom_commune || null
    }));

    console.timeEnd('TOTAL /iris_by_point_lite');
    return res.json({ nb_iris: iris.length, iris });

  } catch (err) {
    console.error('/iris_by_point (lite) error:', err);
    console.timeEnd('TOTAL /iris_by_point_lite');
    return res.status(500).json({ error: 'server' });
  }
});

// ------------------------------------------------------------------
// PING
// ------------------------------------------------------------------
app.get('/ping', async (_req, res) => {
  console.log('Received /ping request');
  try {
    await pool.query('SELECT 1');
    console.log('Database query successful');
    res.json({ message: 'pong', db_status: 'ok', date: new Date() });
  } catch (e) {
    console.error('Error in /ping:', e);
    res.status(500).json({ message: 'pong', db_status: 'error', error: e.message });
  }
});

// ------------------------------------------------------------------
// COLLECTIVITES (NOUVEAU ENDPOINT)
// ------------------------------------------------------------------
// ------------------------------------------------------------------
// ------------------------------------------------------------------
//  POST /collectivites_polygons
//  Corps attendu : [{code_insee, type_collectivite}, …]
//  Réponse       : FeatureCollection GeoJSON
// ------------------------------------------------------------------
app.post('/collectivites_polygons', async (req, res) => {
  const input = req.body;
  if (!Array.isArray(input)) return res.status(400).json({error:'array required'});

  const arr  = input.filter(x => x.type_collectivite === 'arrondissement')
                    .map(x => x.code_insee);
  const com  = input.filter(x => x.type_collectivite === 'commune')
                    .map(x => x.code_insee);
  const dep  = input.filter(x => x.type_collectivite === 'Département')
                    .map(x => x.code_insee);

  const features = [];

  /* -- 1. arrondissements -------------------------------- */
  if (arr.length){
    const sql = `
      SELECT insee_arm AS code,
             ST_AsGeoJSON(ST_Transform(geom_2154,4326)) AS geo
      FROM decoupages.communes
      WHERE insee_arm = ANY($1)
    `;
    const {rows} = await pool.query(sql,[arr]);
    rows.forEach(r => features.push({
      type:'Feature',
      geometry: JSON.parse(r.geo),
      properties:{ code_insee:r.code, type:'arrondissement' }
    }));
  }

  /* -- 2. communes globales (union si P,L,M) -------------- */
  if (com.length){
    const sql = `
      WITH un AS (
        SELECT insee_com,
               ST_Union(geom_2154) AS geom
        FROM decoupages.communes
        WHERE insee_com = ANY($1)
        GROUP BY insee_com
      )
      SELECT insee_com AS code,
             ST_AsGeoJSON(ST_Transform(geom,4326)) AS geo
      FROM un;
    `;
    const {rows} = await pool.query(sql,[com]);
    rows.forEach(r => features.push({
      type:'Feature',
      geometry: JSON.parse(r.geo),
      properties:{ code_insee:r.code, type:'commune' }
    }));
  }

  /* -- 3. départements ------------------------------------ */
  if (dep.length){
    const sql = `
      SELECT insee_dep AS code,
             ST_AsGeoJSON(ST_Transform(geom_2154,4326)) AS geo
      FROM decoupages.departements
      WHERE insee_dep = ANY($1)
    `;
    const {rows} = await pool.query(sql,[dep]);
    rows.forEach(r => features.push({
      type:'Feature',
      geometry: JSON.parse(r.geo),
      properties:{ code_insee:r.code, type:'Département' }
    }));
  }

  res.set('Cache-Control','public,max-age=3600');
  res.json({ type:'FeatureCollection', features });
});

// ------------------------------------------------------------------
// Route Zenmap AI : extraction des critères (sert à tester/debug)
// ------------------------------------------------------------------
app.post('/zenmap_ai/extract', async (req, res) => {
  try {
    const { chat_transcript, zone_recherche } = req.body;

    if (!chat_transcript || typeof chat_transcript !== 'string') {
      return res.status(400).json({
        success: false,
        error: 'chat_transcript manquant ou invalide'
      });
    }

    const extractResult = await runZenmapExtractor(zone_recherche, chat_transcript);
    const rawCriteria = extractResult.criteria || extractResult || {};
    const { zone_recherche: zrFromExtractor, ...criteria } = rawCriteria;

    return res.json({
      success: true,
      source: 'zenmap_ai/extract',
      zone_recherche: zone_recherche || null,
      criteria: extractResult.criteria || extractResult, // selon ton schéma de sortie actuel
      raw: extractResult
    });

  } catch (error) {
    console.error('Erreur dans /zenmap_ai/extract :', error);
    return res.status(500).json({
      success: false,
      error: 'INTERNAL_ERROR_EXTRACTOR',
      detail: error.message
    });
  }
});


// ------------------------------------------------------------------
// Route Zenmap AI : assistant de chat
// ------------------------------------------------------------------
app.post('/zenmap_ai/chat', async (req, res) => {
  try {
    const { conversation } = req.body;

    if (!conversation || typeof conversation !== 'string') {
      return res.status(400).json({
        success: false,
        error: 'Champ "conversation" manquant ou invalide'
      });
    }

    const userContent = [
      "Voici la conversation complète entre l'utilisateur (USER) et toi (ASSISTANT).",
      "Tu dois simplement répondre au DERNIER message de l'utilisateur.",
      "",
      "[CONVERSATION]",
      conversation
    ].join('\n');

    const response = await openai.chat.completions.create({
      model: "gpt-4.1-mini",
      messages: [
        { role: "system", content: CHAT_SYSTEM_PROMPT },
        { role: "user", content: userContent }
      ],
      temperature: 0.4
    });

    const assistantMessage = response.choices[0].message?.content || '';

    // --- nouvelle logique TAG ---
    let action = 'none';
    let cleanedReply = assistantMessage;

    if (assistantMessage.includes('[[ACTION:OPEN_LOCATION]]')) {
      action = 'open_location';
      cleanedReply = assistantMessage
        .replace('[[ACTION:OPEN_LOCATION]]', '')
        .trim();
    }

    // 👇 renvoyer la nouvelle structure
    return res.json({
      success: true,
      reply: cleanedReply,
      action: action
    });

  } catch (err) {
    console.error('Erreur dans /zenmap_ai/chat :', err);
    return res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ------------------------------------------------------------------
// POST /zenmap_ai/match
//  - Entrée :
//      { zone_recherche: { ... }, conversation: "..." }
//  - Étape 1 : appel de l'assistant extracteur -> critères structurés
//  - Étape 2 (à venir) : calcul du matching + requêtes SQL
// ------------------------------------------------------------------
app.post('/zenmap_ai/match', async (req, res) => {
  console.log('>>> [zenmap_ai/match] BODY:', JSON.stringify(req.body, null, 2));

  try {
    const { conversation, zone_recherche } = req.body;

    // 1) Vérifs de base
    if (!conversation || typeof conversation !== 'string') {
      return res.status(400).json({
        success: false,
        error: 'conversation manquante ou invalide'
      });
    }

    if (!zone_recherche || typeof zone_recherche !== 'object') {
      return res.status(400).json({
        success: false,
        error: 'zone_recherche manquante ou invalide'
      });
    }

    // 2) Appel de l’assistant extracteur
    const extractResult = await runZenmapExtractor(zone_recherche, conversation);

    // On normalise ce qu'il renvoie
    const rawCriteria = extractResult.criteria || extractResult || {};

    // On enlève zone_recherche des critères
    const { zone_recherche: zrFromExtractor, ...criteria } = rawCriteria;

    // 3) Calcul du matching
    const matches = await computeMatching(zone_recherche, criteria);

    // 4) Réponse pour Bubble
    return res.json({
      success: true,
      zone_recherche,
      criteria,
      matches,
      debug: {
        raw_extractor_output: extractResult
      }
    });

  } catch (error) {
    console.error('Erreur dans /zenmap_ai/match :', error);
    return res.status(500).json({
      success: false,
      error: 'INTERNAL_ERROR_MATCH',
      detail: error.message
    });
  }
});



// ------------------------------------------------------------------
// LANCEMENT
// ------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API IRIS v3 démarrée sur le port ${PORT}`);
});